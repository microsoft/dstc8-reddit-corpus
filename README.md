# dstc8-reddit

Reddit corpus construction code for the **DSTC 8 Competition, Multi-Domain End-to-End Track, Task 2: Fast Adaptation**.

See the [DSTC 8 website](), [track proposal](http://workshop.colips.org/dstc7/dstc8/DTSC8_multidomain_task_proposal.pdf), and [challenge homepage](https://todo) for more details.

This package is based on [Luigi](https://luigi.readthedocs.io/en/stable/index.html) and downloads raw data from the [3rd party Pushshift repository](https://files.pushshift.io/reddit/).


## Generating the Corpus


### Requirements

- Python 3.5+
- **~210 GB** space for constructing the dialogues with default settings
  - Final zip is only **4.2 GB** though
  - [You can get away with less disk space, ~30GB](https://github.com/microsoft/dstc8-reddit-corpus/#i-dont-have-enough-disk-space)
- An internet connection
- 24-72 hours to generate the data
  - Depends on speed of internet connection, how many cores, how much RAM 
  - On a "beefy" machine with 16+ cores and 64GB+ RAM this should take under two days


### Setup and Generation

1. Modify `run_dir` in `configs/config.prod.yaml` to where you want all your data to be generated.
1. Install the package with `python setup.py install`.
1. Generate the data with `python scripts/reddit.py generate`.


## Corpus Information

- 1000 relatively non-toxic subreddits with over 75,000 subscribers each
- 12 months of data, November 2017 to October 2018 (inclusive)
- Up to two dialogues sampled per post, from different top-level comments
- Additional splits for validation varying date and subreddits with respect to training set
- Dialogues have at least 4 turns each
- Filtering done on Reddit API fields, also bot-like content, etc.
- No post processing done on the corpus. Our preprocessing code will be made public in our baseline model release
- The final dataset zip is approximately **4.2 GB** in size


| Folder | Total Dialogues |
| --- | --- |
| dstc8-reddit-corpus.zip:dialogues/training | 5,085,113 |
| dstc8-reddit-corpus.zip:dialogues/validation_date_in_domain_in | 254,624 |
| dstc8-reddit-corpus.zip:dialogues/validation_date_in_domain_out | 1,278,998 |
| dstc8-reddit-corpus.zip:dialogues/validation_date_out_domain_in  | 1,037,977 |
| dstc8-reddit-corpus.zip:dialogues/validation_date_out_domain_out | 262,036 |


### Schema

The zip file is structured like this:

```yaml
dstc8-reddit-corpus.zip:
  - dialogues/
    - training/                           # From [2017-11, ..., 2018-08] and 920 training subreddits
      - <subreddit>.txt
      ...
    - validation_date_in_subreddit_in/    # From [2017-11, ..., 2018-08] and 920 training subreddits
      # Dialogues are disjoint from those in training
      - <subreddit>.txt
      ...
    - validation_date_in_subreddit_out/   # From [2017-11, ..., 2018-08] and 80 held-out subreddits
      - <subreddit>.txt
      ...
    - validation_date_out_subreddit_in/   # From [2018-09, 2018-10] and 920 training subreddits
      - <subreddit>.txt
      ...
    - validation_date_out_subreddit_out/  # From [2018-09, 2018-10] and 80 held-out subreddits
      - <subreddit>.txt
      ...
  - tasks.txt                             # All subreddits
  - tasks_train.txt                       # Subreddits in the `subreddit_in` subsets
  - tasks_held_out.txt                    # Subreddits in the `subreddit_out` subsets
```


Each `dialogues/<set>` directory contains one file per subreddit, named for the subreddit e.g. `dialogues/training/askreddit.txt`.

Each dialogues file (e.g. `dialogues/training/askreddit.txt`) has one dialogue per line, encoded as stringified JSON with this schema:

```
{
    "id":       "...",  // md5 of the sequence of turn IDs comprising this dialogue
    "domain":   "...",  // subreddit name, lowercase
    "task_id": "...",   // first 8 chars of md5 of the lowercase subreddit name
    "bot_id": "",       // empty string, not valid for reddit
    "user_id": "",      // empty string, not valid for reddit
    "turns": [
        "...",
        ...
    ]
}
```

Here's an example of reading the data in Python:

```python
with zipfile.ZipFile('dstc8-reddit-corpus.zip','r') as myzip:
    with io.TextIOWrapper(myzip.open('dialogues/training/askreddit.txt'), encoding='utf-8') as f:
        for line in f:
            dlg = json.loads(line)
```


## Troubleshooting


#### Testing

You may want to download and subsample a single submissions and comments file from Pushshift to troubleshoot potential issues you may have. Alternatively you can reduce the date range by setting the `manual_dates` parameter in the `config.yaml`. E.g.

```yaml
manual_dates:
  - "2018-02"
```


#### Memory errors

In case you hit your machine's memory limits, you may want to tweak the number of concurrently running tasks in your `config.yaml`. E.g.

```yaml
max_concurrent_build: 6
max_concurrent_sample: 12
```

Dialogue construction and sampling are the most memory intensive.


#### Why does it take so long to download the data

Pushshift enforces a connection limit. In our experience any more than 4 connections per IP and you risk having your connections terminated. 

We default to 4 concurrent connections at once, but if this is too much you can modify the `config.yaml`.

```yaml
max_concurrent_downloads: 4
```


#### Too many open files

This shouldn't happen, but in case you get `IOError: [Errno 24] Too many open files`, try increasing the file open limit to something over a 1000 with `ulimit -n 1000` or unlimited with `ulimit -n unlimited` (on Linux). See [here](https://stackoverflow.com/questions/18280612/ioerror-errno-24-too-many-open-files) for details.


#### I don't have enough disk space

Luigi is basically `make` for Python. It requires the targets from the last task exist to proceed with the next task - but not those previous. So say you've filtered all the submissions and comments - and are now building dialogues - you can delete the raw data if you wish. 

The raw data takes up the most space (>144 GB) but also takes the longest time to obtain, so delete this with caution.

Filtering and building the dialogues discards a lot of the data, so only keeping things in the `dialogues*` directories is safe.

**If you just want the final dataset you can use the `--small` option to delete raw and intermediate data the dataset is generated, e.g.**

```bash
python scripts/reddit.py generate --small
```

#### Windows

This hasn't been thoroughly tested on Windows, but it's dependencies are entirely Python and as far as we know all supported on Linux, Mac OS, and Windows.



# Contributing

This project welcomes contributions and suggestions.  Most contributions require you to agree to a
Contributor License Agreement (CLA) declaring that you have the right to, and actually do, grant us
the rights to use your contribution. For details, visit https://cla.microsoft.com.

When you submit a pull request, a CLA-bot will automatically determine whether you need to provide
a CLA and decorate the PR appropriately (e.g., label, comment). Simply follow the instructions
provided by the bot. You will only need to do this once across all repos using our CLA.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or
contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.
