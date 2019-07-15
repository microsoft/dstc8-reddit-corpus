import gzip
import logging
import luigi
import rapidjson as json

from collections import OrderedDict
from hashlib import md5
from numpy import random

from dstc8_reddit.config import RedditConfig
from dstc8_reddit.constants import Patterns
from dstc8_reddit.tasks.construction import BuildDialogues
from dstc8_reddit.util import delete_requires
from dstc8_reddit.validation import SessionItem


class SingleDialogueFilterer:
  """
  Note token limit precedes char limit, only one can be used.
  """

  def __init__(self, turn_limit=-1, tokens=False, chars=False, min_turns=2):
    self.turn_limit = turn_limit if tokens or chars else -1
    self.limit_tokens = tokens
    self.limit_chars = chars if not tokens else False
    self.min_turns = min_turns

  def _turn_split_fn(self, turn):
    if self.limit_tokens:
      return turn.split()
    return turn

  def __call__(self, dlg_obj):
    try:
      dlg = dlg_obj['turns_with_ids']

      if len(dlg) < self.min_turns:
        return None

      if self.turn_limit > 0:
        # Treat the first turn differently since it has self text
        # We include the whole title, and only apply the limit to the self text
        match = Patterns.GET_SUBMISSION_SELF_TEXT_RGX.search(dlg[0][1])
        if match:
          selftext = match.group().strip()

          if selftext and len(self._turn_split_fn(selftext)) > self.turn_limit:
            return None

        if not all([len(self._turn_split_fn(t)) <= self.turn_limit for _, t in dlg[1:]]):
          return None

      return dlg_obj

    except KeyError:
      return None


TURN_ID = 0
TURN_TEXT = 1


class GrouperSamplerCfg:

  def __init__(self, group_level, n_groups=-1, shuffle_groups=False,
               n_per_group=-1, shuffle_within_groups=False):
    self.group_level = group_level
    self.n_groups = n_groups
    self.shuffle_groups = shuffle_groups
    self.n_per_group = n_per_group
    self.shuffle_within_groups = shuffle_within_groups

  def __repr__(self):
    return '%s: %s' % (self.__class__, self.__dict__)


class DialogueGrouperSampler:

  def __call__(self, dlgs, group_configs):
    if not group_configs:
      return dlgs

    def make_indices(max_n, limit=-1, shuffle=False):
      inds = list(range(max_n))
      if shuffle:
        random.shuffle(inds)
      if limit > 0:
        inds = inds[:limit]
      return inds

    cfg = group_configs.pop(0)
    grouped_dlgs_dict = OrderedDict()

    for dlg in dlgs:
      if len(dlg) <= cfg.group_level:
        continue

      group_key = dlg[cfg.group_level][TURN_ID]

      if group_key not in grouped_dlgs_dict:
        grouped_dlgs_dict[group_key] = []

      grouped_dlgs_dict[group_key].append(dlg)

    all_groups = list(grouped_dlgs_dict.keys())
    inds = make_indices(len(all_groups), cfg.n_groups, cfg.shuffle_groups)
    groups = [all_groups[i] for i in inds]
    final_dlgs = []

    for g in groups:
      sub_grouped_dlgs = self.__call__(
        grouped_dlgs_dict[g], group_configs[:])
      inds = make_indices(len(sub_grouped_dlgs),
                          cfg.n_per_group, cfg.shuffle_within_groups)
      final_sub_grouped_dlgs = [sub_grouped_dlgs[i] for i in inds]
      final_dlgs.extend(final_sub_grouped_dlgs)

    return final_dlgs


class SampleDialogues(luigi.Task):
  date = luigi.Parameter()
  resources = {"max_concurrent_sample": 1}

  def output(self):
    return luigi.LocalTarget(RedditConfig().make_sampled_dialogues_filepath(self.date))

  def requires(self):
    return BuildDialogues(self.date)

  def run(self):
    random.seed(RedditConfig().random_seed)

    turn_limit, limit_chars, limit_tokens = -1, False, False

    if RedditConfig().turn_token_limit > 0:
      turn_limit, limit_tokens = RedditConfig().turn_token_limit, True

    elif RedditConfig().turn_char_limit > 0:
      turn_limit, limit_chars = RedditConfig().turn_char_limit, True

    filterer = SingleDialogueFilterer(
      turn_limit=turn_limit, tokens=limit_tokens, chars=limit_chars,
      min_turns=RedditConfig().min_dialogue_length
    )

    dlgs = []
    submission2subreddit = {}

    # First we need to read all dialogues
    # In the future may need to grouping by subreddit or post id to prevent memory errors
    f = gzip.open(RedditConfig().make_raw_dialogues_filepath(self.date), 'rt', encoding='utf-8')
    for line in f:
      if not len(line.strip()):
        continue

      try:
        dlg_obj = json.loads(line)
      except json.JSONDecodeError as e:
        logging.debug(f"[sample] Error parsing line ({str(e)})\n - {line}")
        continue

      if filterer(dlg_obj) is None:
        continue

      dlgs.append(dlg_obj['turns_with_ids'])
      # Key sub2sub by submission ID
      submission_id = dlg_obj['turns_with_ids'][0][0]
      submission2subreddit[submission_id] = dlg_obj['domain']
    f.close()

    # Next sample and write the dialogues
    with self.output().temporary_path() as tmp_path:
      f = gzip.open(tmp_path, 'wt', encoding='utf-8')

      sampler = DialogueGrouperSampler()
      grouping_configs = [
        # First group by post, don't care about shuffling or group limits
        GrouperSamplerCfg(group_level=0),
        # Next group by a level comment from the top
        # With default parameters: group by top comment, choose one dialogue per top comment group,
        # only 2 top comment groups, with shuffling
        GrouperSamplerCfg(
          group_level=RedditConfig().sampling_group_level,
          n_groups=RedditConfig().sampling_n_groups,
          n_per_group=RedditConfig().sampling_n_per_group,
          shuffle_groups=RedditConfig().sampling_shuffle_groups,
          shuffle_within_groups=RedditConfig().sampling_shuffle_within_groups
        )
      ]
      sampled_dlgs = sampler(dlgs, grouping_configs)

      def to_json(dlg_obj):
        # Validate
        SessionItem(**dlg_obj)
        return json.dumps(dlg_obj)

      # Can't write all dialogues at once else MemoryError
      for i in range(0, len(sampled_dlgs), RedditConfig().dump_interval):
        dlgs_to_write = [to_json({
            'domain': submission2subreddit[d[0][0]],
            'task_id': md5(submission2subreddit[d[0][0]].encode('utf-8')).hexdigest()[:8],
            'turns': [turn for _, turn in d],
            # id is the hash of the joined turn ids
            'id': md5(('_'.join([tid for tid, t in d])).encode('utf-8')).hexdigest(),
            'bot_id': '',
            'user_id': '',
          })
          for d in sampled_dlgs[i:i + RedditConfig().dump_interval]
        ]
        f.write('\n'.join(dlgs_to_write) + '\n')

      f.close()

      logging.debug(f" > [{self.date}] # DLGS: before sample={len(dlgs)}, after sample={len(sampled_dlgs)}")
      lens = [len(d) for d in sampled_dlgs]
      logging.debug(f" > [{self.date}] DLG LENGTHS: max={max(lens)}, min={min(lens)}, avg={sum(lens) / len(lens):2.2f}")

  def on_success(self):
    if RedditConfig().delete_intermediate_data:
      delete_requires(self.requires())