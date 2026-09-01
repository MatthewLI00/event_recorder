import select, sys, termios, tty
import rclpy
from rclpy.node import Node
from std_srvs.srv import Trigger

class KeyboardTrigger(Node):
    def __init__(self):
        super().__init__('keyboard_trigger')
        self.declare_parameter('key', 'r'); self.declare_parameter('trigger_service', '/event_recorder/trigger')
        self.key = str(self.get_parameter('key').value); self.pending = False
        if not sys.stdin.isatty(): raise RuntimeError('keyboard_trigger must run in an interactive terminal')
        self.client = self.create_client(Trigger, str(self.get_parameter('trigger_service').value))
        self.term = termios.tcgetattr(sys.stdin.fileno()); tty.setcbreak(sys.stdin.fileno())
        self.create_timer(.05, self._poll)
    def _poll(self):
        if not self.pending and select.select([sys.stdin], [], [], 0)[0] and sys.stdin.read(1) == self.key:
            if self.client.service_is_ready():
                self.pending = True; self.client.call_async(Trigger.Request()).add_done_callback(self._done)
            else: self.get_logger().warning('Recorder manager service is not ready')
    def _done(self, future):
        self.pending = False
        try: self.get_logger().info(future.result().message)
        except Exception as exc: self.get_logger().error(repr(exc))
    def destroy_node(self):
        termios.tcsetattr(sys.stdin.fileno(), termios.TCSADRAIN, self.term); return super().destroy_node()
def main(args=None):
    rclpy.init(args=args); node = KeyboardTrigger()
    try: rclpy.spin(node)
    except KeyboardInterrupt: pass
    finally: node.destroy_node(); rclpy.try_shutdown()
