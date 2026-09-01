from glob import glob
from setuptools import find_packages, setup


package_name = 'event_recorder'

setup(
    name=package_name,
    version='0.1.0',
    packages=find_packages(exclude=['test']),
    data_files=[
        ('share/ament_index/resource_index/packages', ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),
        ('share/' + package_name + '/config', glob('config/*.yaml')),
        ('share/' + package_name + '/launch', glob('launch/*.launch.py')),
    ],
    install_requires=['setuptools'],
    zip_safe=True,
    maintainer='Event Recorder Maintainer',
    maintainer_email='maintainer@example.com',
    description='Triggered pre/post-event rosbag2 recorder for ROS 2 Humble.',
    license='Apache-2.0',
    entry_points={
        'console_scripts': [
            'recorder_manager = event_recorder.recorder_manager:main',
            'keyboard_trigger = event_recorder.keyboard_trigger:main',
        ],
    },
)
