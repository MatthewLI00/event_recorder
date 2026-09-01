import json
import math
import os
from pathlib import Path
import signal
import subprocess
import time
from typing import Any, Dict, List, Optional

import rclpy
from geometry_msgs.msg import Twist
from nav_msgs.msg import Odometry
from rclpy.executors import MultiThreadedExecutor
from rclpy.node import Node
from rosbag2_interfaces.srv import Snapshot
from std_msgs.msg import String
from std_srvs.srv import Trigger


def _yaw_from_quaternion(x: float, y: float, z: float, w: float) -> float:
    return math.atan2(2.0 * (w * z + x * y), 1.0 - 2.0 * (y * y + z * z))


def _angle_distance(a: float, b: float) -> float:
    return abs(math.atan2(math.sin(a - b), math.cos(a - b)))


class RecorderManager(Node):
    """Keep a rosbag2 ring buffer and persist one bag per trigger window."""

    def __init__(self) -> None:
        super().__init__('event_recorder')
        self._declare_parameters()

        self.post_seconds = float(self.get_parameter('post_trigger_seconds').value)
        self.pre_seconds = float(self.get_parameter('pre_trigger_seconds').value)
        self.cooldown_seconds = float(self.get_parameter('cooldown_seconds').value)
        self.extend_window = bool(
            self.get_parameter('extend_post_window_on_retrigger').value)
        self.output_root = Path(
            str(self.get_parameter('output_directory').value)).expanduser()
        self.output_root.mkdir(parents=True, exist_ok=True)

        self.state = 'BUFFERING'
        self.snapshot_deadline = 0.0
        self.cooldown_deadline = 0.0
        self.pending_events: List[Dict[str, Any]] = []
        self.snapshot_started_monotonic = 0.0
        self.bag_process: Optional[subprocess.Popen] = None
        self.bag_directory: Optional[Path] = None
        self.snapshot_client = self.create_client(
            Snapshot, '/rosbag2_recorder/snapshot')
        self.create_service(Trigger, '~/trigger', self._service_trigger)
        self.create_subscription(
            String, '~/trigger_event', self._topic_trigger, 20)

        self._setup_pose_trigger()
        self.create_timer(0.1, self._tick)
        self.create_timer(1.0, self._health_check)
        self._start_rosbag()

    def _declare_parameters(self) -> None:
        self.declare_parameter('output_directory', '/tmp/event_recordings')
        self.declare_parameter('pre_trigger_seconds', 15.0)
        self.declare_parameter('post_trigger_seconds', 10.0)
        self.declare_parameter('cooldown_seconds', 0.0)
        self.declare_parameter('extend_post_window_on_retrigger', True)
        self.declare_parameter('recorder.topics', ['/rosout', '/tf', '/tf_static'])
        self.declare_parameter('recorder.all_topics', False)
        self.declare_parameter('recorder.record_all', False)
        self.declare_parameter('recorder.disable_discovery', False)
        self.declare_parameter('recorder.bag_name', '')
        self.declare_parameter('recorder.bag_prefix', '')
        self.declare_parameter('recorder.storage_id', 'sqlite3')
        self.declare_parameter('recorder.max_cache_size', 104857600)
        self.declare_parameter('recorder.max_bagfile_size', 0)
        self.declare_parameter('recorder.max_bagfile_duration', 0.0)
        self.declare_parameter('recorder.cache_warning_ratio', 0.8)
        self.declare_parameter('recorder.use_sim_time', False)
        self.declare_parameter('recorder.qos_overrides_path', '')

        self.declare_parameter('triggers.pose_stale.enabled', True)
        self.declare_parameter('triggers.pose_stale.topic', '/odom')
        self.declare_parameter('triggers.pose_stale.message_timeout_seconds', 3.0)
        self.declare_parameter('triggers.pose_stale.stationary_timeout_seconds', 10.0)
        self.declare_parameter('triggers.pose_stale.position_epsilon_m', 0.03)
        self.declare_parameter('triggers.pose_stale.yaw_epsilon_rad', 0.03)
        self.declare_parameter('triggers.pose_stale.require_motion_command', True)
        self.declare_parameter('triggers.pose_stale.cmd_vel_topic', '/cmd_vel')
        self.declare_parameter('triggers.pose_stale.linear_cmd_threshold', 0.05)
        self.declare_parameter('triggers.pose_stale.angular_cmd_threshold', 0.05)
        self.declare_parameter('triggers.pose_stale.cmd_timeout_seconds', 1.0)

    def _start_rosbag(self) -> None:
        self.bag_directory = self._next_bag_directory()
        max_cache_size = int(self.get_parameter('recorder.max_cache_size').value)
        if max_cache_size < 0:
            raise ValueError('recorder.max_cache_size must be >= 0')
        command = [
            'ros2', 'bag', 'record', '--snapshot-mode',
            '--max-cache-size', str(max_cache_size),
            '--storage', str(self.get_parameter('recorder.storage_id').value),
            '--output', str(self.bag_directory),
        ]
        max_bagfile_size = int(
            self.get_parameter('recorder.max_bagfile_size').value)
        if max_bagfile_size < 0:
            raise ValueError('recorder.max_bagfile_size must be >= 0')
        if max_bagfile_size:
            command += ['--max-bag-size', str(max_bagfile_size)]
        max_bagfile_duration = float(
            self.get_parameter('recorder.max_bagfile_duration').value)
        if max_bagfile_duration < 0:
            raise ValueError('recorder.max_bagfile_duration must be >= 0')
        if max_bagfile_duration:
            command += ['--max-bag-duration', str(max_bagfile_duration)]
        qos_path = str(self.get_parameter('recorder.qos_overrides_path').value)
        if qos_path:
            command += ['--qos-profile-overrides-path', qos_path]
        if bool(self.get_parameter('recorder.use_sim_time').value):
            command.append('--use-sim-time')
        if bool(self.get_parameter('recorder.disable_discovery').value):
            command.append('--no-discovery')
        if (bool(self.get_parameter('recorder.record_all').value)
                or bool(self.get_parameter('recorder.all_topics').value)):
            command.append('--all')
        else:
            topics = list(self.get_parameter('recorder.topics').value)
            if '/rosout' not in topics:
                topics.append('/rosout')
            command.extend(topics)

        self.get_logger().info('Starting rosbag2: ' + ' '.join(command))
        self.bag_process = subprocess.Popen(command, start_new_session=True)
        self.get_logger().info(
            'rosbag cache limit: %d bytes (snapshot mode may use up to '
            'approximately %d bytes for double buffering)' %
            (max_cache_size, max_cache_size * 2))

    def _next_bag_directory(self) -> Path:
        """Choose a non-existing output directory, including rapid restarts."""
        bag_name = str(self.get_parameter('recorder.bag_name').value).strip()
        if bag_name:
            if Path(bag_name).name != bag_name:
                raise ValueError(
                    'recorder.bag_name must be a directory name, not a path')
            base_name = bag_name
        else:
            prefix = str(self.get_parameter('recorder.bag_prefix').value)
            if os.path.sep in prefix or (os.path.altsep and os.path.altsep in prefix):
                raise ValueError(
                    'recorder.bag_prefix must be a directory-name prefix, not a path')
            base_name = prefix + time.strftime('%Y%m%d_%H%M%S')
        if not base_name:
            raise ValueError('recorder.bag_name or recorder.bag_prefix is invalid')
        candidate = self.output_root / base_name
        sequence = 1
        while candidate.exists():
            candidate = self.output_root / (base_name + '_' + str(sequence))
            sequence += 1
        if candidate.name != base_name:
            self.get_logger().warning(
                'Bag directory already exists; using ' + str(candidate))
        return candidate

    def _setup_pose_trigger(self) -> None:
        self.pose_enabled = bool(
            self.get_parameter('triggers.pose_stale.enabled').value)
        self.last_pose_rx: Optional[float] = None
        self.pose_anchor_time: Optional[float] = None
        self.pose_anchor: Optional[tuple] = None
        self.last_cmd_rx: Optional[float] = None
        self.motion_requested = False
        self.pose_fault_latched = False
        if not self.pose_enabled:
            return
        self.create_subscription(
            Odometry,
            str(self.get_parameter('triggers.pose_stale.topic').value),
            self._on_odom,
            20,
        )
        if bool(self.get_parameter(
                'triggers.pose_stale.require_motion_command').value):
            self.create_subscription(
                Twist,
                str(self.get_parameter('triggers.pose_stale.cmd_vel_topic').value),
                self._on_cmd_vel,
                20,
            )

    def _on_cmd_vel(self, msg: Twist) -> None:
        self.last_cmd_rx = time.monotonic()
        linear = math.sqrt(msg.linear.x ** 2 + msg.linear.y ** 2 + msg.linear.z ** 2)
        angular = math.sqrt(
            msg.angular.x ** 2 + msg.angular.y ** 2 + msg.angular.z ** 2)
        self.motion_requested = (
            linear >= float(self.get_parameter(
                'triggers.pose_stale.linear_cmd_threshold').value)
            or angular >= float(self.get_parameter(
                'triggers.pose_stale.angular_cmd_threshold').value)
        )

    def _on_odom(self, msg: Odometry) -> None:
        now = time.monotonic()
        self.last_pose_rx = now
        p = msg.pose.pose.position
        q = msg.pose.pose.orientation
        pose = (p.x, p.y, p.z, _yaw_from_quaternion(q.x, q.y, q.z, q.w))
        if self.pose_anchor is None:
            self.pose_anchor = pose
            self.pose_anchor_time = now
            return
        distance = math.sqrt(sum(
            (pose[i] - self.pose_anchor[i]) ** 2 for i in range(3)))
        yaw_change = _angle_distance(pose[3], self.pose_anchor[3])
        if (distance >= float(self.get_parameter(
                'triggers.pose_stale.position_epsilon_m').value)
                or yaw_change >= float(self.get_parameter(
                    'triggers.pose_stale.yaw_epsilon_rad').value)):
            self.pose_anchor = pose
            self.pose_anchor_time = now
            self.pose_fault_latched = False

    def _motion_is_expected(self, now: float) -> bool:
        require = bool(self.get_parameter(
            'triggers.pose_stale.require_motion_command').value)
        if not require:
            return True
        timeout = float(self.get_parameter(
            'triggers.pose_stale.cmd_timeout_seconds').value)
        return bool(
            self.last_cmd_rx is not None
            and now - self.last_cmd_rx <= timeout
            and self.motion_requested
        )

    def _check_pose_trigger(self, now: float) -> None:
        if not self.pose_enabled or self.pose_fault_latched:
            return
        if not self._motion_is_expected(now):
            return
        message_timeout = float(self.get_parameter(
            'triggers.pose_stale.message_timeout_seconds').value)
        stationary_timeout = float(self.get_parameter(
            'triggers.pose_stale.stationary_timeout_seconds').value)
        if self.last_pose_rx is not None and now - self.last_pose_rx >= message_timeout:
            self.pose_fault_latched = True
            self._accept_trigger('pose_stale', 'odometry_message_timeout', {
                'elapsed_seconds': now - self.last_pose_rx,
            })
        elif (self.pose_anchor_time is not None
              and now - self.pose_anchor_time >= stationary_timeout):
            self.pose_fault_latched = True
            self._accept_trigger('pose_stale', 'odometry_value_unchanged', {
                'elapsed_seconds': now - self.pose_anchor_time,
            })

    def _service_trigger(self, _request, response):
        accepted = self._accept_trigger('service', 'manual_service', {})
        response.success = accepted
        response.message = 'trigger accepted' if accepted else 'trigger ignored in cooldown'
        return response

    def _topic_trigger(self, msg: String) -> None:
        try:
            payload = json.loads(msg.data)
            if not isinstance(payload, dict):
                raise ValueError('payload must be an object')
            source = str(payload.pop('source', 'topic'))
            reason = str(payload.pop('reason', 'external'))
            self._accept_trigger(source, reason, payload)
        except (ValueError, TypeError, json.JSONDecodeError) as exc:
            self.get_logger().warning('Invalid trigger_event JSON: ' + str(exc))

    def _accept_trigger(
            self, source: str, reason: str, details: Dict[str, Any]) -> bool:
        now = time.monotonic()
        if self.state == 'COOLDOWN' and now < self.cooldown_deadline:
            return False
        if self.state == 'SNAPSHOTTING':
            self.get_logger().warning('Trigger ignored while snapshot is being written')
            return False
        ros_now = self.get_clock().now().nanoseconds
        event = {
            'source': source,
            'reason': reason,
            'ros_time_ns': ros_now,
            'wall_time': time.strftime('%Y-%m-%dT%H:%M:%S%z'),
            'details': details,
        }
        self.pending_events.append(event)
        new_deadline = now + self.post_seconds
        if self.state in ('BUFFERING', 'COOLDOWN'):
            self.state = 'WAITING_POST'
            self.snapshot_deadline = new_deadline
        elif self.state == 'WAITING_POST' and self.extend_window:
            self.snapshot_deadline = max(self.snapshot_deadline, new_deadline)
        self.get_logger().warning(
            'Recording trigger accepted: ' + source + '/' + reason)
        return True

    def _tick(self) -> None:
        now = time.monotonic()
        self._check_pose_trigger(now)
        if self.state == 'WAITING_POST' and now >= self.snapshot_deadline:
            if not self.snapshot_client.service_is_ready():
                self.get_logger().warning(
                    'Snapshot service is not ready; will retry',
                    throttle_duration_sec=5.0,
                )
                return
            self.state = 'SNAPSHOTTING'
            self.snapshot_started_monotonic = now
            future = self.snapshot_client.call_async(Snapshot.Request())
            future.add_done_callback(self._snapshot_done)
        elif self.state == 'COOLDOWN' and now >= self.cooldown_deadline:
            self.state = 'BUFFERING'

    def _snapshot_done(self, future) -> None:
        success = False
        error = ''
        try:
            success = bool(future.result().success)
        except Exception as exc:  # service errors must not kill the node
            error = repr(exc)
        record = {
            'snapshot_success': success,
            'snapshot_error': error,
            'bag_directory': str(self.bag_directory),
            'pre_trigger_seconds_requested': self.pre_seconds,
            'post_trigger_seconds': self.post_seconds,
            'events': self.pending_events,
            'completed_wall_time': time.strftime('%Y-%m-%dT%H:%M:%S%z'),
        }
        index_path = self.output_root / 'event_index.jsonl'
        try:
            with index_path.open('a', encoding='utf-8') as stream:
                stream.write(json.dumps(record, ensure_ascii=False) + '\n')
        except OSError as exc:
            self.get_logger().error('Failed to write event index: ' + str(exc))
        self.pending_events = []
        if success:
            self.get_logger().info('Snapshot completed: ' + str(self.bag_directory))
            # A rosbag2 snapshot recorder keeps the same output bag for every
            # snapshot.  Rotate it here so the next trigger window is written
            # to a separate bag and cannot include this window's old cache.
            try:
                self._stop_rosbag()
                self._start_rosbag()
            except (OSError, subprocess.SubprocessError) as exc:
                self.get_logger().fatal('Failed to restart rosbag2: ' + repr(exc))
                self.state = 'BUFFERING'
                return
            self.state = 'COOLDOWN'
            self.cooldown_deadline = time.monotonic() + self.cooldown_seconds
        else:
            self.get_logger().error('Snapshot failed: ' + error)
            self.state = 'BUFFERING'

    def _health_check(self) -> None:
        self._log_recorder_memory()
        if self.bag_process is not None and self.bag_process.poll() is not None:
            code = self.bag_process.returncode
            self.bag_process = None
            self.get_logger().fatal('rosbag2 exited unexpectedly, code=' + str(code))

    def _stop_rosbag(self) -> None:
        """Stop the current snapshot recorder without treating it as a crash."""
        process = self.bag_process
        self.bag_process = None
        if process is None or process.poll() is not None:
            return
        self.get_logger().info('Stopping rosbag2 cleanly')
        os.killpg(process.pid, signal.SIGINT)
        try:
            process.wait(timeout=15.0)
        except subprocess.TimeoutExpired:
            self.get_logger().warning('rosbag2 did not stop; sending SIGTERM')
            os.killpg(process.pid, signal.SIGTERM)
            process.wait(timeout=5.0)

    def _log_recorder_memory(self) -> None:
        """Report recorder RSS; rosbag2 does not expose cache occupancy over ROS."""
        process = self.bag_process
        if process is None or process.poll() is not None:
            return
        try:
            with open('/proc/%d/status' % process.pid, encoding='utf-8') as status:
                rss_line = next(line for line in status if line.startswith('VmRSS:'))
            rss_bytes = int(rss_line.split()[1]) * 1024
        except (OSError, StopIteration, ValueError, IndexError):
            return
        cache_limit = int(self.get_parameter('recorder.max_cache_size').value)
        warning_ratio = float(self.get_parameter('recorder.cache_warning_ratio').value)
        self.get_logger().info(
            'rosbag recorder RSS: %d bytes; configured cache limit: %d bytes' %
            (rss_bytes, cache_limit))
        if (cache_limit > 0 and warning_ratio > 0
                and rss_bytes >= cache_limit * warning_ratio):
            self.get_logger().warning(
                'rosbag recorder RSS has reached %.0f%% of the cache limit; '
                'snapshot double-buffering can require up to 2x the cache limit' %
                (rss_bytes * 100.0 / cache_limit))

    def destroy_node(self) -> bool:
        try:
            self._stop_rosbag()
        except (OSError, subprocess.SubprocessError) as exc:
            self.get_logger().error('Failed to stop rosbag2: ' + repr(exc))
        return super().destroy_node()


def main(args=None) -> None:
    rclpy.init(args=args)
    node = RecorderManager()
    executor = MultiThreadedExecutor(num_threads=3)
    executor.add_node(node)
    try:
        executor.spin()
    except KeyboardInterrupt:
        pass
    finally:
        executor.shutdown()
        node.destroy_node()
        rclpy.try_shutdown()


if __name__ == '__main__':
    main()
