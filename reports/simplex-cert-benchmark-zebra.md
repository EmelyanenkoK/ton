# Simplex Certificate Benchmark: Baseline vs ed25519-zebra

Settings: `votes=1..100`, `target-signature-checks=2000`, `warmup-iterations=16`.

- Baseline total: `15431.126 ms`
- Zebra total: `3193.734 ms`
- Overall speedup: `4.832x`

| Votes | Baseline avg cert us | Zebra avg cert us | Speedup |
| ---: | ---: | ---: | ---: |
| 1 | 76.806 | 32.813 | 2.341x |
| 10 | 762.701 | 154.939 | 4.923x |
| 25 | 1901.100 | 389.059 | 4.886x |
| 50 | 3784.150 | 756.122 | 5.005x |
| 75 | 5762.180 | 1173.120 | 4.912x |
| 100 | 7695.940 | 1598.900 | 4.813x |

Artifacts:
- `reports/simplex-cert-benchmark-baseline-current.csv`
- `reports/simplex-cert-benchmark-zebra.csv`
