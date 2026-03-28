Energy measurements were captured using the Linux `powercap` RAPL interface,
which provides hardware energy counters at the package, core, uncore, and DRAM
domains. Samples were collected at regular intervals throughout each experiment
run. The total energy per run is computed as the integral of instantaneous power
over the run duration. Idle baseline power was subtracted where idle baseline
records were available in the `idle_baselines` table.
