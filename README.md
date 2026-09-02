# Event Recorder (ROS 2 Humble)

This package keeps ROS topics in rosbag2 snapshot memory and writes a snapshot
after a configured event. It supports manual service/topic triggers, an
interactive keyboard trigger, and odometry timeout/stall detection.

## Build

From the workspace that contains `recorder/`:

```bash
source /opt/ros/humble/setup.bash
colcon build --symlink-install --packages-select event_recorder
source install/setup.bash
```

Edit `config/recorder.yaml` before running. `recorder.max_cache_size` defaults
to 100 MiB and is passed directly to `ros2 bag record --max-cache-size`.
Snapshot mode uses double buffering, so it can need roughly twice that amount
of memory during a buffer swap. The manager logs the rosbag process RSS every
second and warns after `recorder.cache_warning_ratio` (default 80%) of the
cache limit. RSS includes rosbag overhead; rosbag2 Humble does not expose its
exact in-memory cache occupancy through ROS.

Use `recorder.bag_name` to select a directory name. If it is empty,
`recorder.bag_prefix + YYYYMMDD_HHMMSS` is used; `bag_prefix` is therefore the
recommended setting for repeated start/stop runs. If the selected directory
already exists, the manager appends `_1`, `_2`, and so on rather than letting
rosbag2 exit. `recorder.record_all`, `recorder.disable_discovery`,
`recorder.max_bagfile_size` (bytes), and `recorder.max_bagfile_duration`
(seconds) map to the corresponding `ros2 bag record` options. A value of zero
for either bagfile split setting disables that split condition.

## Run

```bash
ros2 launch event_recorder recorder.launch.py
```

Run the keyboard trigger in a separate interactive terminal:

```bash
source install/setup.bash
ros2 run event_recorder keyboard_trigger
```

The keyboard process does not create or own a rosbag cache. It controls the
cache already owned by `event_recorder`: press `r` once to issue the same trigger
as `/event_recorder/trigger`. The manager writes the retained pre-trigger cache
and automatically waits `post_trigger_seconds` before snapshotting. Pressing
`r` again inside that post-trigger window extends the same bag's endpoint.

Or trigger without a keyboard:

```bash
ros2 service call /event_recorder/trigger std_srvs/srv/Trigger '{}'
```

An external component can publish JSON to `/event_recorder/trigger_event`:

```bash
ros2 topic pub --once /event_recorder/trigger_event std_msgs/msg/String \
  "{data: '{\"source\": \"planner\", \"reason\": \"navigation_failed\"}'}"
```

Snapshots are stored under `output_directory`. `event_index.jsonl` records the
trigger time, reason, snapshot result, and associated bag directory. ROS logs
are captured through `/rosout`; plain stdout/stderr from non-ROS programs are
not part of the bag.

## Recording lifecycle

Launching the node starts a `ros2 bag record --snapshot-mode` child, but this
child only maintains rosbag2's in-memory, byte-bounded ring buffer: no topic
messages are written to a bag before a trigger. A trigger starts (or extends)
the post-trigger deadline. When that deadline passes, the manager snapshots
the buffer, which contains the retained pre-trigger data plus all data through
the last trigger's post-trigger interval. Multiple triggers before that
deadline therefore produce one bag; its end is the latest trigger plus
`post_trigger_seconds`.

After each successful snapshot the manager stops and recreates the rosbag2
snapshot recorder with a new directory. This makes the next trigger window a
separate bag and prevents the previous snapshot's cached data from being
written again. The ring buffer is bounded by bytes, not time: to retain about
`pre_trigger_seconds` of history, size `max_cache_size` for the aggregate topic
bitrate. If the byte limit is reached earlier, rosbag2 discards the oldest
messages first.

## Pose-stale semantics

The detector can report either missing odometry messages or odometry values
that remain within the configured position/yaw epsilon. With
`require_motion_command: true`, it triggers only while a recent `/cmd_vel`
requests motion. Movement clears the fault latch, preventing repeated events
for one continuous fault.
