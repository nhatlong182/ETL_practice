# ETL_practice

Practice read 9gbs logs data generated from movie-watching website in 30 days using PySpark
Finished ETL to clean output (1900000 rows) and imported to MySQL

sample input:
| _id         | _index      | _score     |  _source      | _type      |
| :---        |    :----:   |       ---: |          ---: |       ---: |
|AX_momhia1FFivsGrn9o|history|     0|{KPLUS, HNH579912...|kplus|
|AX_momhca1FFivsGrnvg|history|     0|{KPLUS, HUFD40665...|kplus|
|AX_momhaa1FFivsGrnny|history|     0|{KPLUS, HNH572635...|kplus|
|AX_momhca1FFivsGrnvv|history|     0|{KPLUS, HND141717...|kplus|
|AX_momhia1FFivsGrn98|history|     0|{KPLUS, HNH743103...|kplus|
|AX_momg9a1FFivsGrnkS|history|     0|{KPLUS, HNH893773...|kplus|
|AX_momhca1FFivsGrnwA|history|     0|{KPLUS, HND083642...|kplus|
|AX_momhfa1FFivsGrn2u|history|     0|{KPLUS, DNFD74404...|kplus|
|AX_momhca1FFivsGrnwP|history|     0|{KPLUS, DTFD21200...|kplus|
|AX_momhca1FFivsGrnwU|history|     0|{KPLUS, LDFD05747...|kplus|

| AppName     | Contract    | Mac        |TotalDuration|
| ----------- | ----------- | -----------| ----------- |
|  KPLUS      |HNH579912    |0C96E62FC55C|          254|
|  KPLUS      |HUFD40665    |CCEDDC333614|         1457|
|  KPLUS      |HNH572635    |B068E6A1C5F6|         2318|
|  KPLUS      |HND141717    |08674EE8D2C2|         1452|
|  KPLUS      |HNH743103    |402343C25D7D|          251|
|  KPLUS      |HNH893773    |B84DEE76D3B8|          924|
|  KPLUS      |HND083642    |B84DEE849A0F|         1444|
|  KPLUS      |DNFD74404    |90324BB44C39|          691|
|  KPLUS      |DTFD21200    |B84DEED27709|         1436|
|  KPLUS      |LDFD05747    |0C96E6C95E53|         1434|
|  KPLUS      |HNH063566    |B84DEEDD1C85|          687|
|  KPLUS      |HNH866786    |10394E2790A5|          248|
|  KPLUS      |NBAAA1128    |10394E47C1AF|          247|
|  KPLUS      |HNH960439    |B84DEED34371|          683|
|  KPLUS      |HNJ035736    |CCD4A1FA86A5|          246|
|  KPLUS      |NTFD93673    |B84DEEEF4763|         2288|
|  KPLUS      |HNJ063267    |10394E172CA7|         2282|
|  KPLUS      |HNH790383    |4CEBBD53378B|          906|
|  KPLUS      |THFD12466    |5CEA1D893E1C|          242|
|  KPLUS      |HNH566080    |802BF9E0DDC0|          242|

1 day output:
| Contract    | RelaxDuration| MovieDuration |ChildDuration|SportDuration| TVDuration |Date|
| ----------- | ----------- | ----------- | ----------- |  ----------- | -----------| ----------- |
|HTFD11598    |0            |2884         |0            |0             |707         |2022-04-01   |
|HPFD48556    |69           |0            |0            |0             |92976       |2022-04-01   |
|NBFD10014    |0            |0            |0            |0             |84628       |2022-04-01   |
|HNH619088    |0            |8456         |234          |0             |65210       |2022-04-01   |
|HNH036174    |0            |0            |0            |0             |6049        |2022-04-01   |
|DNH067877    |0            |0            |0            |0             |5760        |2022-04-01   |
|SGH806190    |0            |0            |0            |0             |1131        |2022-04-01   |   
|HNH582022    |0            |0            |0            |0             |86400       |2022-04-01   |
|HNH795510    |0            |5840         |0            |0             |68589       |2022-04-01   |
|DNFD91557    |0            |0            |0            |0             |1640        |2022-04-01   |


Total Duration , Type:
=> Most_Watch

=> Customer Taste

30 days output:
| Contract     | TVDuration | MovieDuration |RelaxDuration| ChildDuration | SportDuration |TotalDuration| latest_date  | report_date | recency |  frequency | most_watch |taste|
| ----------- | ----------- | ----------- | ----------- |  ----------- | -----------| ----------- | ----------- | ----------- | ----------- |  ----------- | -----------| ----------- |
|113.182.209.48|        63|            0|           89|            0|            0|          152| 2022-04-01| 2022-05-01|     30|        1|     Relax|       TV Relax|
|14.182.110.125|       404|            0|           92|            0|            0|          496| 2022-04-10| 2022-05-01|     21|        1|        TV|       TV Relax|
|     AGAAA0338|    278633|            0|            0|            0|            0|       278633| 2022-04-30| 2022-05-01|      1|       30|        TV|             TV|
|     AGAAA0342|    117788|            0|          204|            0|            0|       117992| 2022-04-30| 2022-05-01|      1|       12|        TV|       TV Relax|
|     AGAAA0391|    158931|          129|          373|            0|            0|       159433| 2022-04-30| 2022-05-01|      1|       11|        TV| TV Movie Relax|
|     AGAAA0613|      9377|            0|           26|            0|            0|         9403| 2022-04-30| 2022-05-01|      1|       24|        TV|       TV Relax|
|     AGAAA0638|    227016|            0|            0|            0|            0|       227016| 2022-04-30| 2022-05-01|      1|       30|        TV|             TV|
|     AGAAA0692|    107057|            0|            0|            0|            0|       107057| 2022-04-29| 2022-05-01|      2|        8|        TV|             TV|
|     AGAAA0723|      9279|            0|            0|            0|            0|         9279| 2022-04-29| 2022-05-01|      2|       19|        TV|             TV|
|     AGAAA0729|    161781|            0|            0|            0|            0|       161781| 2022-04-15| 2022-05-01|     16|        3|        TV|             TV|
|     AGAAA0732|     99030|          463|            0|            0|            0|        99493| 2022-04-28| 2022-05-01|      3|       23|        TV|       TV Movie|
|     AGAAA0750|    525708|            0|            0|            0|            0|       525708| 2022-04-30| 2022-05-01|      1|       25|        TV|             TV|
|     AGAAA0848|     12141|            0|            0|            0|            0|        12141| 2022-04-29| 2022-05-01|      2|       25|        TV|             TV|
|     AGAAA0885|    354499|            0|            0|            0|            0|       354499| 2022-04-30| 2022-05-01|      1|       20|        TV|             TV|
|     AGAAA0886|     39163|            0|            0|            0|            0|        39163| 2022-04-28| 2022-05-01|      3|       14|        TV|             TV|
|     AGAAA0901|    796083|            0|            0|            0|            0|       796083| 2022-04-30| 2022-05-01|      1|       30|        TV|             TV|
|     AGAAA1004|     58329|            0|            0|            0|            0|        58329| 2022-04-30| 2022-05-01|      1|       25|        TV|             TV|
|     AGAAA1047|    305816|        53227|            0|            0|            0|       359043| 2022-04-30| 2022-05-01|      1|       30|        TV|       TV Movie|
|     AGAAA1147|   1299590|            0|          103|            0|            0|      1299693| 2022-04-29| 2022-05-01|      2|       23|        TV|       TV Relax|
|     AGAAA1218|    132794|            0|            0|            0|            0|       132794| 2022-04-30| 2022-05-01|      1|        9|        TV|             TV|