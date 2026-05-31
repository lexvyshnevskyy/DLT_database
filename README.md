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
measurements go into measurements (scoped by program_runs.run_id per experiment run)

program_runs table: one row per execution of a program (labels like 4.1, 4.2 for program 4)
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

- During a program run, **core** sends `elapsed_s` on each `measurement_insert` (step-based scheduler clock — same as live UI progress).
- The DB node stores that value and syncs its monotonic anchor so bulk/legacy inserts without `elapsed_s` stay consistent.
- If `elapsed_s` is omitted (older clients), the DB node falls back to run/program monotonic anchors (resume from last row after restart).

So this is the cleanest practical design for current architecture:

no client-side time field
no RTC dependency for normal runs
explicit measurements table
HMI tables remain the source of experiment structure

TODO:
Next step should be wiring core so each control-cycle sample writes directly to measurement_insert / measurement_bulk_insert.