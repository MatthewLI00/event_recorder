from ament_index_python.packages import get_package_share_directory
from launch import LaunchDescription
from launch_ros.actions import Node
import os


def generate_launch_description():
    config = os.path.join(
        get_package_share_directory('event_recorder'),
        'config',
        'recorder.yaml',
    )
    return LaunchDescription([
        Node(
            package='event_recorder',
            executable='recorder_manager',
            name='event_recorder',
            output='screen',
            parameters=[config],
        ),
    ])
