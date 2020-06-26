# PENminer

***COMING SOON: Link to paper, citation, and more documentation!***

Caleb Belth, Xinyi Zheng, and Danai Koutra. _Mining Persistent Activity in Continually Evolving Networks._. Knowledge Discovery and Data Mining (KDD), August 2020. [[Link to the paper]()]

If used, please cite:
```bibtex
```

## Setup

```
$ git clone git@github.com:GemsLab/PENminer.git
$ cd PENminer/test
$ python tester.py
$ cd ../src
```

### Requirements 

- `Python 3`
- `numpy`
- `scipy`
- `rrcf`

## Data

- `eu_email.txt` EU Email network
- `columbus_bike.txt` Columbus Bike network
- `reddit.txt` Reddit network
- `darpa_ip.txt` DARPA IP network (zipped in `darpa_ip.zip`)
- `darpa_ip_without_labels` DARPA IP network with attack edges not marked for fair anomaly detection (zipped in `darpa_ip.zip`)

The other datasets used in the paper were too big to share via Github. Alternatives are being considered.

#### Dataset Format

Each row (edge update) has the format: `{1/-1},{u},{v},{w},{u_label},{v_label},{edge_label},...,{timestamp}`.

Here `1` or `-1` specifies insert or delete, `u` is the id of the first node and `v` of the second. `w` is a weight (1 if unweighted), `u_label` and `v_label` are the nodes' labels (ignored if view != `label`), and `edge_label` is the edge's label (if unlabeled, it doesn't matter what it is, as long as it is the same for all edges). The ... means that other information can be stored (e.g., a string version of the timestamp or some helpful description) that will be ignored by the code. `timestamp` is an integer timestamp in seconds. Edge updates (rows) are assumed to be sorted by timestamp. 

## Example usage (from `src/` dir)

For `reddit` dataset, with `k_max = 1`, `delta_max = 1`, `alpha = 1`, `beta = 0.2`, `gamma = 5.0`, `view = 'id'`

sPENminer:

`python main.py -s reddit -v id -ms 1 -ws 1 --alpha 1.0 --beta 0.2 --gamma 5.0 -v id`

oPENminer:

`python main.py -s reddit -v id -ms 1 -ws 1 --alpha 1.0 --beta 0.2 --gamma 5.0 -v id -o True`

### Arguments

`--stream / -s (Required)` Expects `{stream}.txt` to be in `data/` directory in format as described above.

`--verbose / -v True/False (Optional; Default = True)` Whether or not to print logs while running.

`--window_size / -ws [1, infinity) (Optional; Default = 1)` The window size in seconds (integer) (equivalently the maximum snippet duration delta_max).

`--max_size / -ms {1, 2, 3} (Optional; Default = 1)` The maximum snippet size (k_max). Only implemented for k_max in {1, 2, 3}.

`--view / -v {id, label, order}` the view of the snippet to use. 

`--alpha / -alpha (0, infinity) (Optional; Default = 1)` the exponent for `W(.)`.

`--beta / -beta (0, infinity) (Optional; Default = 1)` the exponent for `F(.)`.

`--gamma / -gamma (0, infinity) (Optional; Default = 1)` the exponent for `S(.)`.

`--offline / -o (Optional; Default = False)` Whether to use sPENminer (if True) or oPENminer (if False).
