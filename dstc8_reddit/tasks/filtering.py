import luigi
import rapidjson as json

from dstc8_reddit.config import RedditConfig
from dstc8_reddit.constants import Patterns, SUBMISSION_ID_PREFIX
from dstc8_reddit.tasks.download import DownloadRawFile
from dstc8_reddit.util import process_file_linewise, SubmissionJsonOutputter, CommentJsonOutputter, delete_requires


class RawSubmissionFilterer:

  def __init__(self):
    self.subreddits = set(RedditConfig().all_subreddits)

  def __call__(self, x):
    """
    `.get(...)` means field may not appear and it's absence shouldn't cause failure
    """
    try:
      subreddit = x['subreddit'].lower()

      if self.subreddits and subreddit not in self.subreddits:
        return None

      if any([x.get('archived', False), x.get('hidden', False), x.get('over_18', False),
              x.get('locked', False)]):
        return None

      if x.get('subreddit_type', 'public') != 'public':
        return None

      if x['num_comments'] <= 0:
        return None

      if x.get('num_crossposts', 0) > 0:
        return None

      if x['score'] < 1:
        return None

      # Filter the bots, we don't care about FPs here
      if Patterns.BOT_AUTHOR_RGX.search(x['author'].lower()):
        return None

      if Patterns.BOT_BODY_RGX.search(x.get('selftext', '')) or Patterns.BOT_BODY_RGX.search(x['title']):
        return None

      if Patterns.BOT_BUTTON_RGX.search(x.get('selftext', '')) or Patterns.BOT_BUTTON_RGX.search(x['title']):
        return None

      if x.get('selftext', '') == '[deleted]' or x.get('selftext', '') == '[removed]':
        return None

      # Media filtering
      if any([x.get('is_reddit_media_domain', False),
              x.get('is_video', False),
              x.get('media', False)]):
        return None

      if 'post_hint' in x and (x['post_hint'].startswith('rich') or x['post_hint'] == 'image'):
        return None

      # Table filtering. Most of these are bot comments that are entirely tables, ignore completely
      if Patterns.DETECT_MARKDOWN_TABLE_RGX.search(x.get('selftext', '')):
        return None

      # Require some word chars at least
      if not Patterns.ALPHANUM_RGX.search(x['title']) \
              and not Patterns.ALPHANUM_RGX.search(x.get('selftext', 'TEST')):
        return None

      return x

    except KeyError:
      return None


class RawCommentFilterer:

  def __init__(self, submission_ids_file=None):
    self.subreddits = set(RedditConfig().all_subreddits)

    if submission_ids_file:
      with open(submission_ids_file, 'r', encoding='utf-8') as f:
        self.submission_ids = set(
          [SUBMISSION_ID_PREFIX + x.strip() for x in f.readlines()])
    else:
      self.submission_ids = set()

  def __call__(self, x):

    try:
      subreddit = x['subreddit'].lower()

      if self.subreddits and subreddit not in self.subreddits:
        return None

      if x['score'] < 1:
        return None

      if self.submission_ids and x['link_id'] not in self.submission_ids:
        return None

      if x['body'] == '[deleted]' or x['body'] == '[removed]':
        return None

      # Filter the bots, we don't care about FPs here
      if Patterns.BOT_AUTHOR_RGX.search(x['author'].lower()):
        return None

      if Patterns.BOT_BODY_RGX.search(x['body']):
        return None

      if Patterns.BOT_BUTTON_RGX.search(x['body']):
        return None

      # Table filtering
      if Patterns.DETECT_MARKDOWN_TABLE_RGX.search(x['body']):
        return None

      # Require some word chars at least
      if not Patterns.ALPHANUM_RGX.search(x['body']):
        return None

      return x

    except KeyError:
      return None


class FilterRawSubmissions(luigi.Task):
  date = luigi.Parameter()

  def output(self):
    return [
      luigi.LocalTarget(
        RedditConfig().make_filtered_filepath(self.date, 'submissions')),
      luigi.LocalTarget(
        RedditConfig().make_ids_filepath(self.date, 'submissions'))
    ]

  def requires(self):
    return DownloadRawFile(self.date, 'submissions')

  def run(self):
    with self.output()[0].temporary_path() as tmp_path, \
            self.output()[1].temporary_path() as tmp_ids_path:
      process_file_linewise(
        in_filepath=RedditConfig().make_raw_filepath(self.date, 'submissions'),
        out_filepath=tmp_path,
        out_ids_filepath=tmp_ids_path,
        parser=lambda x: json.loads(x),
        filterer=RawSubmissionFilterer(),
        outputter=SubmissionJsonOutputter(),
        buffer_size=RedditConfig().dump_interval
      )

  def on_success(self):
    if RedditConfig().delete_intermediate_data:
      delete_requires(self.requires())


class FilterRawComments(luigi.Task):
  date = luigi.Parameter()

  def output(self):
    return luigi.LocalTarget(
        RedditConfig().make_filtered_filepath(self.date, 'comments'))

  def requires(self):
    return [
      DownloadRawFile(self.date, 'comments'),
      FilterRawSubmissions(self.date)
    ]

  def run(self):
    with self.output().temporary_path() as tmp_path:
      process_file_linewise(
        in_filepath=RedditConfig().make_raw_filepath(self.date, 'comments'),
        out_filepath=tmp_path,
        out_ids_filepath=None,
        parser=lambda x: json.loads(x),
        filterer=RawCommentFilterer(
          RedditConfig().make_ids_filepath(self.date, 'submissions')
        ),
        outputter=CommentJsonOutputter(),
        buffer_size=RedditConfig().dump_interval
      )

  def on_success(self):
    if RedditConfig().delete_intermediate_data:
      delete_requires(self.requires())
