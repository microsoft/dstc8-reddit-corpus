from pydantic import BaseModel, validator
from typing import List

from dstc8_reddit.config import RedditConfig
from dstc8_reddit.constants import Patterns


class SessionItem(BaseModel):
  id: str
  user_id: str
  bot_id: str
  domain: str
  task_id: str
  turns: List[str]

  @validator('id', 'task_id', 'domain', pre=True)
  def has_chars(cls, v):
    if len(v) == 0:
      raise ValueError('Zero-length string!')
    return v

  @validator('id', 'task_id', pre=True)
  def is_hex(cls, v):
    int(v, 16)
    return v

  @validator('domain', pre=True)
  def is_lowercase(cls, v):
    if Patterns.ANY_UPPERCASE_RGX.search(v):
      raise ValueError(f"Upper case domain! {v}")
    return v

  @validator('turns', whole=True, pre=True)
  def turns_all_have_chars(cls, v):
    if len(v) < RedditConfig().min_dialogue_length:
      raise ValueError('Not enough turns!')
    if any([(not t.strip()) for t in v]):
      raise ValueError('Zero-length strings in turns!')
    return v
