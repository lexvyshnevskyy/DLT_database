from launch import LaunchDescription
from launch.actions import DeclareLaunchArgument
from launch.substitutions import LaunchConfiguration
from launch_ros.actions import Node


def generate_launch_description():
    endpoint_arg = DeclareLaunchArgument('endpoint', default_value='db')
    publish_rate_arg = DeclareLaunchArgument('publish_rate', default_value='100.0')
    response_endpoint_arg = DeclareLaunchArgument('response_endpoint', default_value='response')
    query_endpoint_arg = DeclareLaunchArgument('query_endpoint', default_value='query')
    db_host_arg = DeclareLaunchArgument('db_host', default_value='127.0.0.1')
    db_port_arg = DeclareLaunchArgument('db_port', default_value='3306')
    db_user_arg = DeclareLaunchArgument('db_user', default_value='ubuntu')
    db_password_arg = DeclareLaunchArgument('db_password', default_value='raspberry')
    db_name_arg = DeclareLaunchArgument('db_name', default_value='exp')
    auto_init_schema_arg = DeclareLaunchArgument('auto_init_schema', default_value='true')

    node = Node(
        package='database',
        executable='run.py',
        namespace='database',
        name=LaunchConfiguration('endpoint'),
        output='screen',
        parameters=[{
            'publish_rate': LaunchConfiguration('publish_rate'),
            'response_endpoint': LaunchConfiguration('response_endpoint'),
            'query_endpoint': LaunchConfiguration('query_endpoint'),
            'db.host': LaunchConfiguration('db_host'),
            'db.port': LaunchConfiguration('db_port'),
            'db.user': LaunchConfiguration('db_user'),
            'db.password': LaunchConfiguration('db_password'),
            'db.name': LaunchConfiguration('db_name'),
            'auto_init_schema': LaunchConfiguration('auto_init_schema'),
        }]
    )

    return LaunchDescription([
        endpoint_arg,
        publish_rate_arg,
        response_endpoint_arg,
        query_endpoint_arg,
        db_host_arg,
        db_port_arg,
        db_user_arg,
        db_password_arg,
        db_name_arg,
        auto_init_schema_arg,
        node,
    ])
