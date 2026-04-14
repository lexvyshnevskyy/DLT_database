# database (ROS 2)

ROS 2 migration of the original ROS 1 `database` package.

## Notes

- Keeps the service type as `database/srv/Query`.
- Uses `ament_cmake` so the package can generate its own service interface.
- Installs the Python sources next to the runnable script to avoid the
  `ament_cmake_python + rosidl_generate_interfaces` conflict in one package.

## Last update

What changed:

tm is removed from the client request
the table now stores elapsed_s instead
elapsed_s is calculated automatically by the DB node
experiment data uses programs, program_temp, program_meta
measurements go into measurements
freq is now numeric (DOUBLE)
program_id is the main field name, but exp_id is still accepted for compatibility

New insert shape:

{
"cmd": "measurement_insert",
"program_id": 123,
"freq": 1000,
"measure_ch1": 12.34,
"measure_ch2": 56.78,
"t_ch1": 320.15,
"t_ch2": 321.45,
"t_exp": 325.00
}

How elapsed_s works now:

first measurement for a program gets time base 0
later rows use monotonic time inside the DB node
so RTC absence is not a problem for normal experiment timing

One important caveat:

if the DB node restarts during an experiment, it resumes elapsed_s approximately from the last stored row
that is good enough for now, but not mathematically perfect across restarts

So this is the cleanest practical design for current architecture:

no client-side time field
no RTC dependency for normal runs
explicit measurements table
HMI tables remain the source of experiment structure

TODO:
Next step should be wiring core so each control-cycle sample writes directly to measurement_insert / measurement_bulk_insert.