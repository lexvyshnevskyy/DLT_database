# database (ROS 2)

ROS 2 migration of the original ROS 1 `database` package.

## Notes

- Keeps the service type as `database/srv/Query`.
- Uses `ament_cmake` so the package can generate its own service interface.
- Installs the Python sources next to the runnable script to avoid the
  `ament_cmake_python + rosidl_generate_interfaces` conflict in one package.
