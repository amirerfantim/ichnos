# Reference generators

These scripts produce the golden values baked into the Rust tests. They exist so
that the numbers in `src/geo.rs`, `src/locate/fit.rs` and `src/locate/cluster.rs`
can be regenerated and checked, rather than trusted.

```bash
python3 -m venv venv && ./venv/bin/pip install numpy scipy
./venv/bin/python fit_reference.py          # fits and cluster groupings
python3 geodesy_reference.py                # distances (no dependencies)
```

- `geodesy_reference.py` — a direct transcription of Mozilla Ichnaea's
  `geocalclib/geocalc.pyx`: Vincenty on the WGS-84 ellipsoid with a Haversine
  fallback. Used to check `src/geo.rs`.
- `fit_reference.py` — runs the real `scipy.optimize.leastsq` over Ichnaea's own
  objective, and the real `scipy.cluster.hierarchy` complete-linkage clustering.
  Used to check `src/locate/`.

One deliberate difference is recorded in `fit.rs`: where MINPACK's relative-step
test stops early on a scale mismatch, this implementation runs to convergence.
The tests assert that our objective value is never worse than scipy's.
