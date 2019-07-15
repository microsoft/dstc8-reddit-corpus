import os
import yaml

from datetime import datetime
from enum import IntEnum
from functools import wraps
from pydantic import BaseModel
from typing import Dict, List, Set


class Subset(IntEnum):
  TRAINING = 0
  VALIDATION_DATE_IN_DOMAIN_IN = 1
  VALIDATION_DATE_IN_DOMAIN_OUT = 2
  VALIDATION_DATE_OUT_DOMAIN_IN = 3
  VALIDATION_DATE_OUT_DOMAIN_OUT = 4

  def __str__(self):
    return self.name.lower().replace('Subset', '')

  @staticmethod
  def get_date_in_train():
    return set([Subset.TRAINING, Subset.VALIDATION_DATE_IN_DOMAIN_IN, Subset.VALIDATION_DATE_IN_DOMAIN_OUT])

  @staticmethod
  def get_date_out_train():
    return set([Subset.VALIDATION_DATE_OUT_DOMAIN_IN, Subset.VALIDATION_DATE_OUT_DOMAIN_OUT])

  @staticmethod
  def get_domain_in_train():
    return set([Subset.TRAINING, Subset.VALIDATION_DATE_IN_DOMAIN_IN, Subset.VALIDATION_DATE_OUT_DOMAIN_IN])

  @staticmethod
  def get_domain_out_train():
    return set([Subset.VALIDATION_DATE_IN_DOMAIN_OUT, Subset.VALIDATION_DATE_OUT_DOMAIN_OUT])


def parse_date(date):
  try:
    dt_value = datetime.strptime(date, '%Y-%m')
  except ValueError:
    raise ValueError('Invalid date format, needs to be "YYYY-MM"!')
  return dt_value


def check_date(f):
  @wraps(f)
  def wrapper(*args, **kwargs):
    # Get self in the decorator
    self, date = args[0], kwargs.get('date', args[1])
    dt_value = parse_date(date)

    if dt_value.year < self.min_year or dt_value.year > self.max_year:
      raise ValueError('Invalid year in date!')

    if dt_value.year == self.max_year and dt_value.month > self.max_year_max_month:
      raise ValueError('Month is newer than max!')

    return f(*args, **kwargs)
  return wrapper


def check_filetype(f):
  @wraps(f)
  def wrapper(*args, **kwargs):
    filetype = kwargs.get('filetype', args[2])
    if filetype != 'comments' and filetype != 'submissions':
      raise ValueError(f"Invalid filetype `{filetype}`, must be `submissions` or `comments`!")
    return f(*args, **kwargs)
  return wrapper


def check_split(f):
  @wraps(f)
  def wrapper(*args, **kwargs):
    split = kwargs['split'] if 'split' in kwargs else args[2]
    if split not in Subset:
      raise ValueError(f"Invalid set `{split}`, must be one of {list(Subset)}!")
    return f(*args, **kwargs)
  return wrapper


class RawConfig(BaseModel):
  is_dev: bool = False
  is_test: bool = False
  run_dir: str = 'processed'
  data_base_dir: str = 'reddit'
  data_raw_dir: str = 'raw'
  data_filtered_dir: str = 'filtered'
  data_raw_dialogues_dir: str = 'dialogues'
  data_sampled_dialogues_dir: str = 'dialogues_sampled'
  data_splits_dir: str = 'dialogues_split'
  data_zip_path: str = 'dstc8-reddit-corpus.zip'
  submissions_url_template: str = 'https://files.pushshift.io/reddit/submissions/RS_%s.%s'
  comments_url_template: str = 'https://files.pushshift.io/reddit/comments/RC_%s.%s'
  submissions_checksum_url_template: str = 'https://files.pushshift.io/reddit/submissions/sha256sums.txt'
  comments_checksum_url_template: str = 'https://files.pushshift.io/reddit/comments/sha256sum.txt'
  min_year: int = 2017
  max_year: int = 2018
  min_year_min_month: int = 11
  max_year_max_month: int = 10
  # Since checksums aren't available for submissions & comments in the same month
  submissions_xz_starts_year: int = 2017
  submissions_xz_starts_month: int = 11
  comments_xz_starts_year: int = 2017
  comments_xz_starts_month: int = 12
  download_chunk_size: int = 4 * 2**10
  min_dialogue_length: int = 4
  turn_char_limit: int = -1
  turn_token_limit: int = -1
  random_seed: int = 1001
  sampling_group_level: int = 1
  sampling_n_groups: int = 2
  sampling_n_per_group: int = 1
  sampling_shuffle_groups: bool = True
  sampling_shuffle_within_groups: bool = True
  dump_interval: int = 2**10
  max_concurrent_downloads: int = 4
  max_concurrent_build: int = 6
  max_concurrent_sample: int = 12
  train_boundary_month: int = 9
  train_boundary_year: int = 2018
  train_sampling_pr: float = 20. / 21.  # 1/21 goes to the date_in_domain_in validation set
  cat_buffer_size: int = 2 ** 20
  manual_dates: List[str] = []
  all_subreddits: Set[str] = set()
  held_out_subreddits: Set[str] = set()
  # Pushshift is missing some checksums
  missing_checksums: Dict[str, str] = {
    'RC_2018-06.xz': '01778d656253b5497769eeab36c8610a64b6f271fbe4065cdc21f5841faee530',
    'RC_2018-07.xz': 'e703b5b95005d655283ae7149ee775a31534402fa801705fc771c32eea874781',
    'RC_2018-08.xz': 'b8939ecd280b48459c929c532eda923f3a2514db026175ed953a7956744c6003',
    'RC_2018-10.xz': 'cadb242a4b5f166071effdd9adbc1d7a78c978d3622bc01cd0f20d3a4c269bd0',
  }
  delete_intermediate_data: bool = False


class RedditConfig:
  """ This essentially becomes a factory with all the true config info stored at the class level. """
  _cfg = None

  @classmethod
  def initialize(cls, cfgyaml=None, extra_config=None):
    kwargs = {}
    if cfgyaml:
      with open(cfgyaml, 'r', encoding='utf-8') as f:
        kwargs = yaml.load(f, Loader=yaml.FullLoader)

    # Will override any preset and anything from the config yaml
    if extra_config:
      kwargs.update(extra_config)

    kwargs['all_subreddits'] = set(kwargs.get('all_subreddits', []))
    kwargs['held_out_subreddits'] = set(kwargs.get('held_out_subreddits', []))

    cfg = RawConfig(**kwargs)

    # Extra cleanup steps
    cfg.data_base_dir = os.path.join(cfg.run_dir, cfg.data_base_dir)
    cfg.data_raw_dir = os.path.join(cfg.data_base_dir, cfg.data_raw_dir)
    cfg.data_filtered_dir = os.path.join(cfg.data_base_dir, cfg.data_filtered_dir)
    cfg.data_raw_dialogues_dir = os.path.join(cfg.data_base_dir, cfg.data_raw_dialogues_dir)
    cfg.data_sampled_dialogues_dir = os.path.join(cfg.data_base_dir, cfg.data_sampled_dialogues_dir)
    cfg.data_splits_dir = os.path.join(cfg.data_base_dir, cfg.data_splits_dir)
    cfg.data_zip_path = os.path.join(cfg.data_base_dir, cfg.data_zip_path)

    cfg.all_subreddits = set(d.lower() for d in cfg.all_subreddits)
    cfg.held_out_subreddits = set(d.lower() for d in cfg.held_out_subreddits)

    cls._cfg = cfg

  def __init__(self):
    if self._cfg is None:
      raise RuntimeError("RedditConfig was not initialized!")
    for field, val in self._cfg:
      setattr(self, field, val)

  def __str__(self):
    return 'RedditConfig:\n' + '\n'.join([f" - {k}={v}" for k, v in self.__dict__.items()])

  def make_all_dates(self):
    if self.manual_dates:
      _ = [parse_date(d) for d in self.manual_dates]
      return self.manual_dates

    dates = []
    for year in [self.min_year, self.max_year]:
      for month in range(1, 13, 1):
        if (year == self.min_year and month < self.min_year_min_month) \
                or (year == self.max_year and month > self.max_year_max_month):
          continue
        dates.append(f"{year:04d}-{month:02d}")
    return dates

  def make_all_dates_filetypes(self):
    dates = self.make_all_dates()
    return [(d, 'comments') for d in dates] + [(d, 'submissions') for d in dates]

  @check_date
  @check_filetype
  def make_source_url(self, date, filetype):
    dt = parse_date(date)
    extension = 'bz2'

    if filetype == 'submissions':
      xz_starts_year, xz_starts_month = self.submissions_xz_starts_year, self.submissions_xz_starts_month
    else:
      xz_starts_year, xz_starts_month = self.comments_xz_starts_year, self.comments_xz_starts_month

    if (dt.year > xz_starts_year) or (dt.year == xz_starts_year and dt.month >= xz_starts_month):
      extension = 'xz'

    if filetype == 'submissions':
      return self.submissions_url_template % (date, extension)
    else:
      return self.comments_url_template % (date, extension)

  @check_date
  @check_filetype
  def make_raw_filepath(self, date, filetype):
    dt = parse_date(date)
    prefix = 'RS' if filetype == 'submissions' else 'RC'
    extension = 'bz2'

    if filetype == 'submissions':
      xz_starts_year, xz_starts_month = self.submissions_xz_starts_year, self.submissions_xz_starts_month
    else:
      xz_starts_year, xz_starts_month = self.comments_xz_starts_year, self.comments_xz_starts_month

    if (dt.year > xz_starts_year) or (dt.year == xz_starts_year and dt.month >= xz_starts_month):
      extension = 'xz'

    os.makedirs(self.data_raw_dir, exist_ok=True)
    return os.path.join(self.data_raw_dir, f"{prefix}_{date}.{extension}")

  @check_date
  @check_filetype
  def make_filtered_filepath(self, date, filetype):
    prefix = 'RS' if filetype == 'submissions' else 'RC'
    os.makedirs(self.data_filtered_dir, exist_ok=True)
    return os.path.join(self.data_filtered_dir, f"{prefix}_{date}.txt.gz")

  @check_date
  @check_filetype
  def make_ids_filepath(self, date, filetype):
    prefix = 'RS' if filetype == 'submissions' else 'RC'
    os.makedirs(self.data_filtered_dir, exist_ok=True)
    return os.path.join(self.data_filtered_dir, f"{prefix}_{date}.ids.txt")

  @check_date
  def make_raw_dialogues_filepath(self, date):
    os.makedirs(self.data_raw_dialogues_dir, exist_ok=True)
    return os.path.join(self.data_raw_dialogues_dir, f"DLGS_{date}.txt.gz")

  @check_date
  def make_sampled_dialogues_filepath(self, date):
    os.makedirs(self.data_sampled_dialogues_dir, exist_ok=True)
    return os.path.join(self.data_sampled_dialogues_dir, f"DLGS_{date}.txt.gz")

  @check_date
  @check_split
  def make_split_date_domain_path(self, date, split, domain):
    split_dir = os.path.join(self.data_splits_dir, str(split))
    os.makedirs(split_dir, exist_ok=True)
    return os.path.join(split_dir, f"{date}_{domain}.txt.gz")

  @check_split
  def make_split_domain_path(self, split, domain):
    split_dir = os.path.join(self.data_splits_dir, str(split))
    os.makedirs(split_dir, exist_ok=True)
    return os.path.join(split_dir, f"{domain}.txt")

  @check_date
  def is_date_in_train(self, date):
    dt = parse_date(date)
    if (dt.year > self.train_boundary_year) \
            or (dt.year == self.train_boundary_year and dt.month >= self.train_boundary_month):
      return False
    return True

  def make_zip_path(self):
    os.makedirs(os.path.dirname(self.data_zip_path), exist_ok=True)
    return self.data_zip_path
