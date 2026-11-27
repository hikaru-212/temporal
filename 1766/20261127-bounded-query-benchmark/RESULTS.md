# Full performance results

All latencies are milliseconds. Each version/scenario/size has 120 measured samples across six process rounds; the 30 warm-ups remain in [timing.csv](timing.csv) but are excluded from statistics. No outliers or decision branches are removed.

Speedup = V2 median / V4.2 median; below 1 means V4.2 is slower. Delta = V4.2 median − V2 median. Both intervals are paired-round 95% bootstrap intervals. Median/P95 values are pooled across rounds, not averages of round medians. Query and next-WFT medians need not sum to the directly measured combined median.

Query: mock poll yields query → query response. Next WFT: query response → next WFT completion (including handoff). Combined: query yield → WFT completion. Normal WFT has no query (N/A), and combined equals next-WFT latency measured from its poll. Held LA ends at the completion carrying RecordMarker. The additional raw next_poll_ms field excludes the query-to-next-poll handoff.

## History sizes

| Timer cycles | Normal / non-effectful / trailing | Cancellation | Held LA |
|---:|---:|---:|---:|
| 18 | 98 | 104 | 103 |
| 198 | 998 | 1,004 | 1,003 |
| 998 | 4,998 | 5,004 | 5,003 |
| 1,998 | 9,998 | 10,004 | 10,003 |

Each timer cycle contributes five events and one WFT. Tails add 8 events / 2 WFTs for the first three scenarios, 14 / 3 for cancellation, and 13 / 3 for held LA. The full history populates the cache before timing; the next task is delivered as sticky incremental history, requiring a mock full-history fetch on eviction.

## Normal WFT

| Events | Metric | V2 median | V2 P95 | V4.2 median | V4.2 P95 | Speedup | 95% CI | Delta | Delta 95% CI |
|---:|---|---:|---:|---:|---:|---:|---|---:|---|
| 98 | next_wft_ms | 0.1362 | 0.2161 | 0.1380 | 0.2390 | 0.9873× | 0.8784–1.0763× | +0.0018 | -0.0098–+0.0181 |
| 98 | combined_ms | 0.1362 | 0.2161 | 0.1380 | 0.2390 | 0.9873× | 0.8784–1.0763× | +0.0018 | -0.0098–+0.0181 |
| 998 | next_wft_ms | 0.1277 | 0.1825 | 0.1237 | 0.1765 | 1.0327× | 0.9966–1.0761× | -0.0040 | -0.0094–+0.0004 |
| 998 | combined_ms | 0.1277 | 0.1825 | 0.1237 | 0.1765 | 1.0327× | 0.9966–1.0761× | -0.0040 | -0.0094–+0.0004 |
| 4,998 | next_wft_ms | 0.1349 | 0.1920 | 0.1389 | 0.2091 | 0.9710× | 0.9445–1.0315× | +0.0040 | -0.0044–+0.0078 |
| 4,998 | combined_ms | 0.1349 | 0.1920 | 0.1389 | 0.2091 | 0.9710× | 0.9445–1.0315× | +0.0040 | -0.0044–+0.0078 |
| 9,998 | next_wft_ms | 0.1436 | 0.2067 | 0.1432 | 0.1940 | 1.0026× | 0.9809–1.0240× | -0.0004 | -0.0034–+0.0027 |
| 9,998 | combined_ms | 0.1436 | 0.2067 | 0.1432 | 0.1940 | 1.0026× | 0.9809–1.0240× | -0.0004 | -0.0034–+0.0027 |

## Non-effectful legacy query

| Events | Metric | V2 median | V2 P95 | V4.2 median | V4.2 P95 | Speedup | 95% CI | Delta | Delta 95% CI |
|---:|---|---:|---:|---:|---:|---:|---|---:|---|
| 98 | query_ms | 0.2408 | 0.3847 | 0.2596 | 0.3985 | 0.9275× | 0.8913–0.9716× | +0.0188 | +0.0072–+0.0300 |
| 98 | next_wft_ms | 0.1647 | 0.3207 | 0.1657 | 0.2858 | 0.9938× | 0.9420–1.0445× | +0.0010 | -0.0071–+0.0101 |
| 98 | combined_ms | 0.4119 | 0.6555 | 0.4290 | 0.6230 | 0.9602× | 0.8990–0.9825× | +0.0171 | +0.0074–+0.0456 |
| 998 | query_ms | 0.3120 | 0.3870 | 0.3154 | 0.3967 | 0.9890× | 0.9631–1.0071× | +0.0035 | -0.0023–+0.0117 |
| 998 | next_wft_ms | 0.1528 | 0.2212 | 0.1587 | 0.2357 | 0.9627× | 0.9252–0.9841× | +0.0059 | +0.0025–+0.0123 |
| 998 | combined_ms | 0.4690 | 0.5750 | 0.4792 | 0.6057 | 0.9787× | 0.9482–1.0218× | +0.0102 | -0.0104–+0.0251 |
| 4,998 | query_ms | 0.7835 | 0.8823 | 0.7757 | 0.8893 | 1.0100× | 0.9805–1.0341× | -0.0078 | -0.0256–+0.0156 |
| 4,998 | next_wft_ms | 0.1729 | 0.2443 | 0.1732 | 0.2478 | 0.9982× | 0.9714–1.0249× | +0.0003 | -0.0042–+0.0051 |
| 4,998 | combined_ms | 0.9600 | 1.1038 | 0.9553 | 1.0706 | 1.0049× | 0.9854–1.0284× | -0.0047 | -0.0268–+0.0144 |
| 9,998 | query_ms | 1.3381 | 1.4995 | 1.3646 | 1.5087 | 0.9806× | 0.9543–1.0162× | +0.0265 | -0.0220–+0.0624 |
| 9,998 | next_wft_ms | 0.2106 | 0.2790 | 0.2096 | 0.2709 | 1.0044× | 0.9397–1.0269× | -0.0009 | -0.0056–+0.0130 |
| 9,998 | combined_ms | 1.5489 | 1.7099 | 1.5812 | 1.7344 | 0.9796× | 0.9606–1.0136× | +0.0323 | -0.0214–+0.0623 |

## Trailing inputs

| Events | Metric | V2 median | V2 P95 | V4.2 median | V4.2 P95 | Speedup | 95% CI | Delta | Delta 95% CI |
|---:|---|---:|---:|---:|---:|---:|---|---:|---|
| 98 | query_ms | 0.2834 | 0.4782 | 0.2647 | 0.3941 | 1.0707× | 1.0252–1.1540× | -0.0187 | -0.0415–-0.0068 |
| 98 | next_wft_ms | 3.3883 | 4.2905 | 0.1800 | 0.3174 | 18.8242× | 17.0989–20.2147× | -3.2083 | -3.4088–-3.0585 |
| 98 | combined_ms | 3.6784 | 4.7462 | 0.4456 | 0.6696 | 8.2558× | 7.5659–8.8268× | -3.2328 | -3.4910–-3.0839 |
| 998 | query_ms | 0.3434 | 0.7261 | 0.3119 | 0.4023 | 1.1010× | 1.0066–1.9745× | -0.0315 | -0.2983–-0.0020 |
| 998 | next_wft_ms | 27.1371 | 28.3804 | 0.1555 | 0.2249 | 174.5423× | 172.6794–176.0956× | -26.9816 | -27.0792–-26.8591 |
| 998 | combined_ms | 27.5633 | 28.9882 | 0.4821 | 0.5799 | 57.1744× | 55.1122–60.1797× | -27.0812 | -27.2532–-26.9631 |
| 4,998 | query_ms | 0.7737 | 0.8878 | 0.7494 | 0.9363 | 1.0325× | 0.9678–1.1690× | -0.0243 | -0.1124–+0.0258 |
| 4,998 | next_wft_ms | 135.6330 | 140.1625 | 0.1734 | 0.2583 | 782.0529× | 751.1429–816.1939× | -135.4596 | -135.9451–-134.9626 |
| 4,998 | combined_ms | 136.4489 | 140.9636 | 0.9255 | 1.1345 | 147.4369× | 137.3503–160.6152× | -135.5235 | -136.0059–-135.0539 |
| 9,998 | query_ms | 1.3703 | 1.6798 | 1.3640 | 1.5605 | 1.0046× | 0.9684–1.0514× | -0.0063 | -0.0676–+0.0440 |
| 9,998 | next_wft_ms | 276.0647 | 298.4842 | 0.2156 | 0.2682 | 1280.2080× | 1260.1321–1310.5218× | -275.8491 | -276.8354–-274.7513 |
| 9,998 | combined_ms | 277.6317 | 299.8736 | 1.5787 | 1.7918 | 175.8556× | 171.2060–180.8069× | -276.0529 | -276.9439–-274.7682 |

## Interleaved cancellation

| Events | Metric | V2 median | V2 P95 | V4.2 median | V4.2 P95 | Speedup | 95% CI | Delta | Delta 95% CI |
|---:|---|---:|---:|---:|---:|---:|---|---:|---|
| 104 | query_ms | 0.2780 | 0.4129 | 0.2746 | 0.3972 | 1.0126× | 0.9036–1.1191× | -0.0035 | -0.0308–+0.0281 |
| 104 | next_wft_ms | 3.3132 | 4.1991 | 3.3665 | 4.3894 | 0.9842× | 0.8851–1.0522× | +0.0533 | -0.1699–+0.4095 |
| 104 | combined_ms | 3.5940 | 4.5999 | 3.6759 | 4.6943 | 0.9777× | 0.8847–1.0501× | +0.0819 | -0.1764–+0.4363 |
| 1,004 | query_ms | 0.3306 | 0.6218 | 0.3470 | 0.6634 | 0.9529× | 0.9185–0.9936× | +0.0164 | +0.0021–+0.0289 |
| 1,004 | next_wft_ms | 27.3627 | 28.7308 | 27.3376 | 28.9112 | 1.0009× | 0.9962–1.0066× | -0.0251 | -0.1810–+0.1040 |
| 1,004 | combined_ms | 27.7237 | 29.0863 | 27.6897 | 29.3925 | 1.0012× | 0.9934–1.0069× | -0.0340 | -0.1911–+0.1852 |
| 5,004 | query_ms | 0.7336 | 0.9170 | 0.7233 | 0.8593 | 1.0144× | 0.9751–1.0696× | -0.0104 | -0.0510–+0.0184 |
| 5,004 | next_wft_ms | 136.4533 | 146.5754 | 137.0076 | 149.1408 | 0.9960× | 0.9869–1.0063× | +0.5544 | -0.8633–+1.7905 |
| 5,004 | combined_ms | 137.1833 | 147.4188 | 137.7487 | 149.8483 | 0.9959× | 0.9870–1.0058× | +0.5654 | -0.8056–+1.7870 |
| 10,004 | query_ms | 1.3410 | 1.4868 | 1.4049 | 1.5923 | 0.9545× | 0.9234–0.9760× | +0.0639 | +0.0335–+0.1092 |
| 10,004 | next_wft_ms | 274.9937 | 283.3697 | 274.3079 | 281.7087 | 1.0025× | 0.9962–1.0059× | -0.6858 | -1.6219–+1.0394 |
| 10,004 | combined_ms | 276.3281 | 284.6928 | 275.6619 | 283.0872 | 1.0024× | 0.9958–1.0057× | -0.6663 | -1.5718–+1.1559 |

## Held LA result

| Events | Metric | V2 median | V2 P95 | V4.2 median | V4.2 P95 | Speedup | 95% CI | Delta | Delta 95% CI |
|---:|---|---:|---:|---:|---:|---:|---|---:|---|
| 103 | query_ms | 0.3312 | 0.4537 | 805.6403 | 810.2524 | 0.0004× | 0.0004–0.0004× | +805.3091 | +804.8096–+806.0462 |
| 103 | next_wft_ms | 3.5926 | 4.4019 | 0.7393 | 2.0347 | 4.8593× | 4.6530–5.9225× | -2.8533 | -2.9899–-2.7971 |
| 103 | combined_ms | 3.9594 | 4.8103 | 806.5303 | 811.3104 | 0.0049× | 0.0047–0.0051× | +802.5709 | +801.9741–+803.0664 |
| 1,003 | query_ms | 0.4289 | 0.6684 | 805.4219 | 809.7515 | 0.0005× | 0.0005–0.0007× | +804.9930 | +804.6213–+805.4675 |
| 1,003 | next_wft_ms | 28.0256 | 30.3102 | 0.8053 | 1.5678 | 34.8034× | 31.5879–41.0998× | -27.2203 | -27.3246–-27.1379 |
| 1,003 | combined_ms | 28.4894 | 30.8814 | 806.4054 | 810.7675 | 0.0353× | 0.0352–0.0356× | +777.9160 | +777.3467–+778.4436 |
| 5,003 | query_ms | 0.7356 | 0.9105 | 805.4202 | 808.3245 | 0.0009× | 0.0009–0.0010× | +804.6845 | +804.3886–+804.9240 |
| 5,003 | next_wft_ms | 136.9671 | 142.7522 | 0.9593 | 1.8420 | 142.7835× | 125.8545–162.7708× | -136.0078 | -136.3807–-135.5415 |
| 5,003 | combined_ms | 137.7661 | 143.4799 | 806.3760 | 809.2250 | 0.1708× | 0.1702–0.1713× | +668.6099 | +668.2102–+669.2620 |
| 10,003 | query_ms | 3.9187 | 4.4509 | 808.7391 | 812.3938 | 0.0048× | 0.0048–0.0049× | +804.8205 | +803.8760–+805.3783 |
| 10,003 | next_wft_ms | 278.4220 | 298.2826 | 0.9452 | 1.7870 | 294.5578× | 285.6483–299.6661× | -277.4768 | -280.3248–-276.5076 |
| 10,003 | combined_ms | 282.2871 | 302.6783 | 810.0650 | 813.4814 | 0.3485× | 0.3471–0.3525× | +527.7778 | +524.0167–+529.1723 |

## Operation counts

Totals across the 120 measured samples per version/cell. LA executions include the initial setup execution; re-executions count only extra executions after eviction. Shutdown evictions and warm-ups are excluded.

| Scenario | Events | Evictions V2 / V4.2 | History fetches V2 / V4.2 | LA executions V2 / V4.2 | LA re-executions V2 / V4.2 |
|---|---:|---:|---:|---:|---:|
| Normal WFT | 98 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |
| Normal WFT | 998 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |
| Normal WFT | 4,998 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |
| Normal WFT | 9,998 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |
| Non-effectful legacy query | 98 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |
| Non-effectful legacy query | 998 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |
| Non-effectful legacy query | 4,998 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |
| Non-effectful legacy query | 9,998 | 0 / 0 | 0 / 0 | 0 / 0 | 0 / 0 |
| Trailing inputs | 98 | 120 / 0 | 120 / 0 | 0 / 0 | 0 / 0 |
| Trailing inputs | 998 | 120 / 0 | 120 / 0 | 0 / 0 | 0 / 0 |
| Trailing inputs | 4,998 | 120 / 0 | 120 / 0 | 0 / 0 | 0 / 0 |
| Trailing inputs | 9,998 | 120 / 0 | 120 / 0 | 0 / 0 | 0 / 0 |
| Interleaved cancellation | 104 | 120 / 120 | 120 / 120 | 0 / 0 | 0 / 0 |
| Interleaved cancellation | 1,004 | 120 / 120 | 120 / 120 | 0 / 0 | 0 / 0 |
| Interleaved cancellation | 5,004 | 120 / 120 | 120 / 120 | 0 / 0 | 0 / 0 |
| Interleaved cancellation | 10,004 | 120 / 120 | 120 / 120 | 0 / 0 | 0 / 0 |
| Held LA result | 103 | 120 / 0 | 120 / 0 | 240 / 120 | 120 / 0 |
| Held LA result | 1,003 | 120 / 0 | 120 / 0 | 240 / 120 | 120 / 0 |
| Held LA result | 5,003 | 120 / 0 | 120 / 0 | 240 / 120 | 120 / 0 |
| Held LA result | 10,003 | 120 / 0 | 120 / 0 | 240 / 120 | 120 / 0 |

## Round medians

Combined milliseconds, V2 / V4.2 in each cell. Rounds are zero-indexed in the CSV. For per-round median/P95 of every timing metric, including next_poll_ms, run `python3 analyze.py --rounds` (CSV on stdout).

| Scenario | Events | Round 0 | Round 1 | Round 2 | Round 3 | Round 4 | Round 5 |
|---|---:|---:|---:|---:|---:|---:|---:|
| Normal WFT | 98 | 0.1283 / 0.1542 | 0.1359 / 0.1356 | 0.1307 / 0.1505 | 0.1325 / 0.1304 | 0.1457 / 0.1400 | 0.1432 / 0.1238 |
| Normal WFT | 998 | 0.1241 / 0.1287 | 0.1341 / 0.1259 | 0.1190 / 0.1171 | 0.1251 / 0.1211 | 0.1311 / 0.1304 | 0.1398 / 0.1226 |
| Normal WFT | 4,998 | 0.1300 / 0.1318 | 0.1468 / 0.1424 | 0.1330 / 0.1476 | 0.1441 / 0.1321 | 0.1399 / 0.1403 | 0.1340 / 0.1336 |
| Normal WFT | 9,998 | 0.1412 / 0.1408 | 0.1477 / 0.1491 | 0.1421 / 0.1439 | 0.1464 / 0.1408 | 0.1429 / 0.1451 | 0.1396 / 0.1455 |
| Non-effectful legacy query | 98 | 0.3939 / 0.4181 | 0.4137 / 0.4205 | 0.4554 / 0.4885 | 0.4007 / 0.4894 | 0.4146 / 0.4241 | 0.3866 / 0.3801 |
| Non-effectful legacy query | 998 | 0.5079 / 0.5068 | 0.4456 / 0.4758 | 0.4410 / 0.4592 | 0.4922 / 0.4794 | 0.4671 / 0.4772 | 0.4820 / 0.4582 |
| Non-effectful legacy query | 4,998 | 0.9533 / 0.9432 | 0.9634 / 0.9240 | 0.9910 / 1.0075 | 0.9997 / 0.9976 | 0.9425 / 0.9267 | 0.9368 / 0.9255 |
| Non-effectful legacy query | 9,998 | 1.5425 / 1.5692 | 1.6323 / 1.5540 | 1.5650 / 1.6007 | 1.5439 / 1.6025 | 1.4962 / 1.5619 | 1.5264 / 1.6007 |
| Trailing inputs | 98 | 3.8563 / 0.5250 | 3.7042 / 0.4190 | 4.0519 / 0.4832 | 4.0697 / 0.4991 | 3.4795 / 0.4433 | 3.5123 / 0.4226 |
| Trailing inputs | 998 | 27.5234 / 0.5101 | 27.4410 / 0.4807 | 27.8224 / 0.4580 | 27.4590 / 0.4808 | 27.5751 / 0.4893 | 27.4563 / 0.4876 |
| Trailing inputs | 4,998 | 135.8063 / 0.9984 | 137.2591 / 0.8860 | 137.1143 / 0.8945 | 136.5034 / 1.0071 | 136.3327 / 0.9231 | 135.7321 / 0.8190 |
| Trailing inputs | 9,998 | 275.6990 / 1.6236 | 278.3376 / 1.5274 | 276.7381 / 1.5865 | 277.6317 / 1.5053 | 279.5109 / 1.6103 | 275.5484 / 1.5676 |
| Interleaved cancellation | 104 | 3.8451 / 3.9262 | 3.9748 / 3.8510 | 3.5911 / 3.5596 | 3.4850 / 3.7281 | 3.6276 / 3.3268 | 3.2707 / 4.0090 |
| Interleaved cancellation | 1,004 | 27.8065 / 27.6294 | 27.5029 / 27.7941 | 27.7981 / 28.0028 | 27.8810 / 27.9385 | 27.6694 / 27.3938 | 27.5516 / 27.6639 |
| Interleaved cancellation | 5,004 | 136.2533 / 138.0397 | 136.9931 / 136.0236 | 141.6309 / 138.5434 | 137.4753 / 136.8808 | 136.7902 / 138.1775 | 135.6554 / 137.9646 |
| Interleaved cancellation | 10,004 | 276.5696 / 275.8422 | 276.4575 / 276.4564 | 275.1648 / 277.4761 | 275.8689 / 274.4784 | 276.2966 / 277.4366 | 277.1703 / 275.3053 |
| Held LA result | 103 | 3.7512 / 806.1913 | 3.7823 / 806.5105 | 3.9560 / 806.8421 | 4.3085 / 805.8015 | 4.0243 / 805.8344 | 4.0127 / 808.0467 |
| Held LA result | 1,003 | 28.3472 / 806.9943 | 28.3981 / 806.4451 | 28.6576 / 806.3055 | 28.6595 / 805.8208 | 28.7724 / 806.3234 | 28.3407 / 806.4236 |
| Held LA result | 5,003 | 137.3735 / 805.5674 | 138.5953 / 806.3406 | 138.2237 / 806.9757 | 137.0939 / 806.5235 | 137.4087 / 807.0058 | 137.9559 / 805.7509 |
| Held LA result | 10,003 | 281.1475 / 810.8986 | 283.6200 / 807.1105 | 284.6604 / 809.4049 | 280.5464 / 810.4376 | 293.2249 / 809.7186 | 281.7471 / 810.3223 |

## Independent correctness and Held-LA decisions

[correctness.csv](correctness.csv) contains 40/40 passing independent version/scenario/size cases: each live completion is committed to history and replayed on a fresh worker, with nondeterminism and final state/decision checks. Timing samples do not run cold replay. Performance is not the correctness oracle.

| Held-LA events | Measured V2 true / false | Measured V4.2 true / false | Independent V2 decision | Independent V4.2 decision |
|---:|---:|---:|---|---|
| 103 | 58 / 62 | 120 / 0 | true | true |
| 1,003 | 57 / 63 | 120 / 0 | false | true |
| 5,003 | 60 / 60 | 120 / 0 | true | true |
| 10,003 | 61 / 59 | 120 / 0 | true | true |

True means the LA result was visible when the acknowledgement decision ran (timer branch); false selects the memo branch. V2 re-execution introduces a new timing choice, so either branch may be committed. Each independent case matches its own cold replay; cross-version decision equality is not claimed. All timing branches are retained.

## Statistical and semantic limits

P95 is nearest rank: sorted[ceil(0.95 × n) − 1]. Bootstrap resampling draws six whole round indices with replacement, uses those same indices for both versions, and recomputes pooled medians. There are 10,000 resamples, random.Random(183542) reset for each cell/metric; the sorted endpoints are indices 250 and 9749 (zero-based), matching the original analysis. No multiple-comparison adjustment is applied. Six independent process rounds limit precision; only about six observations per cell lie above P95.

Trailing inputs and held LA have different query visibility: V2 observes the trailing acknowledgement; V4.2 defers it to the next WFT. Cancellation exposes its resolution and forces eviction in both versions. Cache retention is not a general latency improvement: the held-LA query waits for the heartbeat despite avoiding a rebuild.

This mock server adds no network, storage, serialization, or server-queue delay to history fetches. The synthetic Rust language driver is not a production language SDK. Initial cache replay, fixture construction, shutdown, compilation and correctness replay are outside timing. LA bodies return immediately, so real LA service time and side effects are not represented. These intervals describe this shared-VM campaign, not production-wide latency or throughput.
