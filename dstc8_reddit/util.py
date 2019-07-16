import bz2
import gzip
import logging
import lzma
import os
import rapidjson as json

from dstc8_reddit.constants import Patterns, OUTPUT_FIELDS, SELF_BREAK_TOKEN, SUBMISSION_ID_PREFIX, COMMENT_ID_PREFIX


class ReducedLineJsonOutputter:
  def __call__(self, data):
    return json.dumps({k: data[k] for k in OUTPUT_FIELDS if k in data})


class CommentJsonOutputter(ReducedLineJsonOutputter):
  def __call__(self, data):
    data['id'] = COMMENT_ID_PREFIX + data['id']
    return super().__call__(data)


class SubmissionJsonOutputter(ReducedLineJsonOutputter):
  def __call__(self, data):
    if data['selftext'].strip():
      data['body'] = '%s %s %s' % (
        data['title'], SELF_BREAK_TOKEN, data['selftext'])
    else:
      data['body'] = data['title']
    data['id'] = SUBMISSION_ID_PREFIX + data['id']
    return super().__call__(data)


def process_file_linewise(
  in_filepath,
  out_filepath,
  out_ids_filepath=None,
  parser=lambda x: x,
  filterer=lambda x: True,
  outputter=lambda x: x,
  buffer_size=-1
):

  def make_file_handle(fp, mode):
    enc = 'utf-8' if 't' in mode else None

    if Patterns.BZ2_EXT_RGX.search(fp):
      return bz2.open(fp, mode, encoding=enc)
    elif Patterns.XZ_EXT_RGX.search(fp):
      return lzma.open(fp, mode, encoding=enc)
    elif Patterns.GZ_EXT_RGX.search(fp):
      return gzip.open(fp, mode, encoding=enc)
    elif Patterns.TXT_TSV_EXT_RGX.search(fp):
      return open(fp, mode, encoding=enc)
    else:
      raise NotImplementedError('Can''t decode file:', fp)

  infile = make_file_handle(in_filepath, 'rt')
  outfile = make_file_handle(out_filepath, 'wt')

  processed_lines = []
  ids_set = set()

  for line in infile:
    if not line.strip():
      continue

    try:
      parsed = parser(line)
    except Exception as e:
      logging.debug(f"[filter] Error parsing line ({str(e)})\n - {line}")
      continue

    res = filterer(parsed)
    if res is None:
      continue

    if out_ids_filepath:
      ids_set.add(res['id'])

    processed_lines.append(outputter(res))

    if buffer_size > 0 and len(processed_lines) >= buffer_size:
      outfile.write('\n'.join(processed_lines) + '\n')
      processed_lines = []

  infile.close()
  if processed_lines:
    outfile.write('\n'.join(processed_lines) + '\n')
  outfile.close()

  if out_ids_filepath:
    with make_file_handle(out_ids_filepath, 'wt') as ids_outfile:
      ids_outfile.write('\n'.join(list(ids_set)) + '\n')


def delete_requires(requires):
  if not isinstance(requires, list):
    requires = [requires]

  for req in requires:
    outputs = req.output()
    if not isinstance(outputs, list):
      outputs = [outputs]

    for out in outputs:
      fp = out.path
      if os.path.exists(fp):
        logging.info(f"[delete] Removed `{fp}`")
        os.remove(fp)
