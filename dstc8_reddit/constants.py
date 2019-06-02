import re

OUTPUT_FIELDS = [r'id', r'body', r'subreddit', r'link_id', r'parent_id']
SUBMISSION_ID_PREFIX = r't3_'
COMMENT_ID_PREFIX = r't1_'
SELF_BREAK_TOKEN = r'<selfbr>'
LINE_BREAK_TOKEN = r'<br>'


class Patterns:
  SELF_BREAK_RGX = re.compile(SELF_BREAK_TOKEN)

  GET_SUBMISSION_SELF_TEXT_RGX = re.compile(
      r'(?<=%s).*$' % SELF_BREAK_TOKEN, re.DOTALL)

  BOT_BODY_RGX = re.compile(
      r"""^i am a bot|^i\'m a bot|^bleep.*?bloop|^beep.*?boop|i am a bot[^a-zA-Z]*$
      |^i\'m a bot[^a-zA-Z]*$|bleep.*?bloop[^a-zA-Z]*$|beep.*?boop[^a-zA-Z]*$'""",
      re.I)
  BOT_BUTTON_RGX = re.compile(r'\^\|\s*\^\[')
  BOT_AUTHOR_PATTERNS = [
      r'^imgur',
      r'^linkfixer',
      r'bots?[^a-zA-Z]*$',
      r'tips?$',
      r'quotes$',
      r'transcriber$',
      r'watch$',
      r'breaker$',
      r'fixer$',
  ]
  BOT_AUTHOR_RGX = re.compile('|'.join(BOT_AUTHOR_PATTERNS), re.I)

  # Markdown tables: https://www.markdownguide.org/extended-syntax/#tables
  DETECT_MARKDOWN_TABLE_RGX = re.compile(r'(\|\s*:?--*:?\s*\|)|(\+----*)')
  ALPHANUM_RGX = re.compile(r'[a-zA-Z0-9]')
  ANY_UPPERCASE_RGX = re.compile(r'[A-Z]')

  BZ2_EXT_RGX = re.compile(r'\.(bz2|bzip2)(\-luigi\-tmp\-\d*)?$')
  XZ_EXT_RGX = re.compile(r'\.xz(\-luigi\-tmp\-\d*)?$')
  GZ_EXT_RGX = re.compile(r'\.(gz|gzip)(\-luigi\-tmp\-\d*)?$')
  TXT_TSV_EXT_RGX = re.compile(r'\.(txt|tsv)(\-luigi\-tmp\-\d*)?$')
