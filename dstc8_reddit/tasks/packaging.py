import gzip
import itertools
import logging
import luigi
import os
import rapidjson as json
import shutil

from collections import defaultdict
from contextlib import ExitStack
from io import TextIOWrapper
from numpy import random
from zipfile import ZipFile, ZIP_DEFLATED

from dstc8_reddit.config import RedditConfig, Subset
from dstc8_reddit.tasks.sampling import SampleDialogues
from dstc8_reddit.util import delete_requires


class SplitDialogues(luigi.Task):
  """ This is a heavy task, but needed to do the splitting required for reaggregation. """
  date = luigi.Parameter()

  def __init__(self, *args, **kwargs):
    super().__init__(*args, **kwargs)
    random.seed(RedditConfig().random_seed)
    self._date_in_train = RedditConfig().is_date_in_train(self.date)

    self._subset_subreddit_combos = list(itertools.product(
      list(Subset),
      RedditConfig().all_subreddits))

  def _make_outputs(self):
    # We take care of 0B files later
    return [(s, d, luigi.LocalTarget(RedditConfig().make_split_date_domain_path(self.date, s, d)))
            for s, d in self._subset_subreddit_combos]

  def output(self):
    # We'll clean up 0B files later
    return [target for _, _, target in self._make_outputs()]

  def requires(self):
    return SampleDialogues(self.date)

  def run(self):
    # Open a lot of files
    with ExitStack() as stack:
      outfiles = {(s, d): stack.enter_context(t.temporary_path())
                  for s, d, t in self._make_outputs()}

      buffers = defaultdict(list)
      infile = gzip.open(RedditConfig().make_sampled_dialogues_filepath(self.date), 'rt', encoding='utf-8')

      for line in infile:
        try:
          dlg = json.loads(line)
        except json.DecodeError as e:
          logging.debug(f"[split] Error parsing line ({str(e)})\n - {line}")
          continue

        domain = dlg['domain']
        key = None

        if self._date_in_train and domain in RedditConfig().held_out_subreddits:
          key = (Subset.VALIDATION_DATE_IN_DOMAIN_OUT, domain)

        elif not self._date_in_train and domain in RedditConfig().held_out_subreddits:
          key = (Subset.VALIDATION_DATE_OUT_DOMAIN_OUT, domain)

        elif not self._date_in_train and domain in RedditConfig().all_subreddits:
          key = (Subset.VALIDATION_DATE_OUT_DOMAIN_IN, domain)

        elif domain in RedditConfig().all_subreddits and random.rand() < RedditConfig().train_sampling_pr:
          key = (Subset.TRAINING, domain)

        elif domain in RedditConfig().all_subreddits:
          key = (Subset.VALIDATION_DATE_IN_DOMAIN_IN, domain)

        else:
          continue

        # Avoid extra encode by storing the raw lines
        buffers[key].append(line)

        if len(buffers[key]) >= RedditConfig().dump_interval:
          with gzip.open(outfiles[key], 'at', encoding='utf-8') as outfile:
            outfile.write(''.join(buffers[key]))
          buffers[key] = []

      # Iterate over outfiles to touch/create even empty files else luigi's temp path complains
      for key, outfilefp in outfiles.items():
        with gzip.open(outfiles[key], 'at', encoding='utf-8') as outfile:
          if key in buffers and len(buffers[key]) > 0:
            outfile.write(''.join(buffers[key]))

  def on_success(self):
    if RedditConfig().delete_intermediate_data:
      delete_requires(self.requires())


class MergeDialoguesOverDates(luigi.Task):
  """
  Decompresses because haven't tested thread safety in zip.
  Also cannot call `delete_requires` in `on_success()` since there are cross-dependencies.
  """
  split = luigi.IntParameter()
  subreddit = luigi.Parameter()

  def output(self):
    return luigi.LocalTarget(RedditConfig().make_split_domain_path(
      split=self.split, domain=self.subreddit))

  def requires(self):
    return [SplitDialogues(d) for d in RedditConfig().make_all_dates()]

  def run(self):
    with self.output().temporary_path() as outpath:
      outfile = open(outpath, 'wt', encoding='utf-8')
      sources = [RedditConfig().make_split_date_domain_path(d, self.split, self.subreddit)
                 for d in RedditConfig().make_all_dates()]
      for src in sources:
        with gzip.open(src, 'rt', encoding='utf-8') as infile:
          shutil.copyfileobj(infile, outfile, RedditConfig().cat_buffer_size)
      outfile.close()


class ZipDataset(luigi.Task):

  def output(self):
    return luigi.LocalTarget(RedditConfig().make_zip_path())

  def requires(self):
    return [MergeDialoguesOverDates(s, d) for s, d in
            itertools.product(list(Subset), RedditConfig().all_subreddits)]

  def run(self):
    with self.output().temporary_path() as zip_path:
      archive = ZipFile(zip_path, 'w', compression=ZIP_DEFLATED)
      for src in self.input():
        if os.stat(src.path).st_size == 0:
          continue
        dest = os.path.join('dialogues', *src.path.split(os.sep)[-2:])
        archive.write(src.path, arcname=dest)

      tasks_to_write = [
          ('tasks.txt', RedditConfig().all_subreddits),
          ('tasks_train.txt', RedditConfig().all_subreddits - RedditConfig().held_out_subreddits),
          ('tasks_held_out.txt', RedditConfig().held_out_subreddits)
      ]

      def make_json_for_subreddit(subreddit):
        return json.dumps({
          'domain': subreddit,
          'task_id': subreddit,
          'bot_prompt': '',
          'user_prompt': '',
          'bot_role': '',
          'user_role': '',
        })

      for fp, tasks, in tasks_to_write:
        with TextIOWrapper(archive.open(fp, 'w'), encoding='utf-8') as f:
          f.write('\n'.join([make_json_for_subreddit(t) for t in sorted(tasks)]) + '\n')

      archive.close()

  def on_success(self):
    if RedditConfig().delete_intermediate_data:
      delete_requires(self.requires())

    parent_reqs = self.requires()
    if not isinstance(parent_reqs, list):
      parent_reqs = [parent_reqs]

    for r in parent_reqs:
      delete_requires(r.requires())
