import luigi
import requests

from hashlib import sha256

from dstc8_reddit.config import RedditConfig


def get_reference_checksum(src_url):
  *_, filename = src_url.split('/')

  if filename in RedditConfig().missing_checksums:
    return RedditConfig().missing_checksums[filename]

  checksums_url = RedditConfig().submissions_checksum_url_template if filename.startswith('RS') \
      else RedditConfig().comments_checksum_url_template

  r = requests.get(checksums_url)
  if r.status_code != 200:
    raise RuntimeError(f"Couldn't get checksums from {checksums_url}, status={r.status_code}")

  checksum = None

  for line in r.content.decode('utf-8').split('\n'):
    if filename in line:
      checksum, *_ = line.split()
      break

  if not checksum:
    raise RuntimeError(f"Couldn't get checksum for {filename}")

  return checksum


class DownloadRawFile(luigi.Task):
  date = luigi.Parameter()
  filetype = luigi.Parameter()
  resources = {"max_concurrent_downloads": 1}

  def output(self):
    dest_fp = RedditConfig().make_raw_filepath(self.date, self.filetype)
    return luigi.LocalTarget(dest_fp)

  def run(self):
    with self.output().temporary_path() as tmp_path:
      src_url = RedditConfig().make_source_url(self.date, self.filetype)
      ref_checksum = get_reference_checksum(src_url)

      r = requests.get(src_url, stream=True)
      if r.status_code != 200:
        raise RuntimeError(f"Error downloading {src_url}, status={r.status_code}")

      m = sha256()
      f = open(tmp_path, 'wb')
      for chunk in r.iter_content(chunk_size=RedditConfig().download_chunk_size):
        if chunk:
          f.write(chunk)
          m.update(chunk)
      f.close()

      checksum = m.hexdigest()

      if checksum != ref_checksum:
        raise RuntimeError(f"Checksums don't match for {'RC' if self.filetype == 'comments' else 'RS'}_{self.date}!")
