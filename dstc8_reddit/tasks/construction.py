import gzip
import logging
import luigi
import rapidjson as json

from dstc8_reddit.config import RedditConfig
from dstc8_reddit.tasks.filtering import FilterRawSubmissions, FilterRawComments
from dstc8_reddit.util import delete_requires


class BuildDialogues(luigi.Task):
  date = luigi.Parameter()
  resources = {"max_concurrent_build": 1}

  def output(self):
    return luigi.LocalTarget(RedditConfig().make_raw_dialogues_filepath(self.date))

  def requires(self):
    return [FilterRawSubmissions(self.date), FilterRawComments(self.date)]

  def run(self):
    turns = dict()
    submission2subreddit = {}

    with gzip.open(RedditConfig().make_filtered_filepath(self.date, 'submissions'),
                   'rt', encoding='utf-8') as f:
      for line in f:
        parsed = json.loads(line)
        rid, content, subreddit = parsed['id'], parsed['body'], parsed['subreddit']
        submission2subreddit[rid] = subreddit
        turns[rid] = (content, rid, False)

    with gzip.open(RedditConfig().make_filtered_filepath(self.date, 'comments'),
                   'rt', encoding='utf-8') as f:
      for line in f:
        try:
          parsed = json.loads(line)
        except json.JSONDecodeError as e:
          logging.debug(f"[build] Error parsing line ({str(e)})\n - {line}")
          continue

        rid, content, parent_comment_id = parsed['id'], parsed['body'], parsed['parent_id']

        # Third element of tuple represents if the turn has a child or not
        this_content, this_parent, this_has_child = content, parent_comment_id, False
        if rid in turns:
          _, _, this_has_child = turns[rid]
        turns[rid] = (
          this_content, this_parent, this_has_child)

        ref_content, ref_parent, ref_has_child = '', '', True
        if parent_comment_id in turns:
          ref_content, ref_parent, _ = turns[parent_comment_id]
        turns[parent_comment_id] = (
          ref_content, ref_parent, ref_has_child)

    with self.output().temporary_path() as tmp_path:
      f = gzip.open(tmp_path, 'wt', encoding='utf-8')
      dlgs_to_write = []

      for rid in turns:
        # Start building the dialogue from the leaf. Also ignore empty turns (placeholder)
        content, parent, has_child = turns[rid]
        if not content or has_child:
          continue

        dlg, ids = [], []
        while True:
          dlg.append(content)
          ids.append(rid)
          try:
            rid = parent
            content, parent, has_child = turns[rid]
            if not content or len(content.strip()) == 0:
              dlg = []
          except KeyError:
            dlg = []
          finally:
            if not dlg or not has_child:
              break
            if rid == parent:
              dlg.append(content)
              ids.append(rid)
              break

        # Some validation
        if not dlg or len(dlg) < RedditConfig().min_dialogue_length or not all(t.strip() for t in dlg):
          continue

        if not ids or len(ids) != len(dlg) or not all(i.strip() for i in ids):
          continue

        try:
          # Lowercase the subreddit
          subreddit = submission2subreddit[ids[-1]].strip().lower()
        except KeyError:
          continue

        if not subreddit:
          continue

        dlg_obj = {
          'domain': subreddit,
          'turns_with_ids': list(zip(ids, dlg))[::-1],
        }

        dlgs_to_write.append(json.dumps(dlg_obj) + '\n')

        if len(dlgs_to_write) >= RedditConfig().dump_interval:
          f.write(''.join(dlgs_to_write))
          dlgs_to_write = []

      # Flush
      if dlgs_to_write:
        f.write(''.join(dlgs_to_write))
      f.close()

  def on_success(self):
    if RedditConfig().delete_intermediate_data:
      delete_requires(self.requires())
