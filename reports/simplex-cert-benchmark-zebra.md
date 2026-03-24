# Simplex Certificate Benchmark: Baseline vs ed25519-zebra

Settings: `votes=1..100`, `target-signature-checks=2000`, `warmup-iterations=16`.

- Baseline total: `15431.126 ms`
- Zebra total: `3117.620 ms`
- Overall speedup: `4.950x`

| Votes | Baseline avg cert us | Zebra avg cert us | Speedup |
| ---: | ---: | ---: | ---: |
| 1 | 76.806 | 32.862 | 2.337x |
| 10 | 762.701 | 151.185 | 5.045x |
| 17 | 1297.800 | 253.766 | 5.114x |
| 25 | 1901.100 | 374.720 | 5.073x |
| 50 | 3784.150 | 735.431 | 5.145x |
| 67 | 5085.610 | 1025.860 | 4.957x |
| 75 | 5762.180 | 1140.270 | 5.053x |
| 100 | 7695.940 | 1564.660 | 4.919x |

Artifacts:
- `reports/simplex-cert-benchmark-baseline-current.csv`
- `reports/simplex-cert-benchmark-zebra.csv`
- `third-party/ton-consensus-ed25519-batch-rs`
