#!/usr/bin/env python3
import click
import luigi

from multiprocessing import cpu_count

from dstc8_reddit.config import RedditConfig
from dstc8_reddit.tasks import DownloadRawFile, ZipDataset


@click.group()
def cli():
  pass


@cli.command('download')
@click.option('-w', '--workers', type=int, default=cpu_count())
@click.option('-c', '--config', type=click.Path(dir_okay=False, file_okay=True, exists=True),
              default='configs/config.prod.yaml')
@click.option('-l', '--log-level', default='ERROR')
def download(workers, config, log_level):
  RedditConfig.initialize(config)
  print(RedditConfig())

  luigi.configuration.get_config().set('resources', 'max_concurrent_downloads',
                                       str(RedditConfig().max_concurrent_downloads))

  result = luigi.interface.build(
    [DownloadRawFile(d, ft) for d, ft in RedditConfig().make_all_dates_filetypes()],
    workers=workers,
    local_scheduler=True,
    log_level=log_level,
    detailed_summary=True,
  )
  print(result.summary_text)


@cli.command('generate')
@click.option('-w', '--workers', type=int, default=cpu_count())
@click.option('-c', '--config', type=click.Path(dir_okay=False, file_okay=True, exists=True),
              default='configs/config.prod.yaml')
@click.option('-l', '--log-level', default='ERROR')
def generate(workers, config, log_level):
  RedditConfig.initialize(config)
  print(RedditConfig())

  luigi.configuration.get_config().set('resources', 'max_concurrent_downloads',
                                       str(RedditConfig().max_concurrent_downloads))
  luigi.configuration.get_config().set('resources', 'max_concurrent_build',
                                       str(RedditConfig().max_concurrent_build))
  luigi.configuration.get_config().set('resources', 'max_concurrent_sample',
                                       str(RedditConfig().max_concurrent_sample))

  result = luigi.interface.build(
    [ZipDataset()],
    workers=workers,
    local_scheduler=True,
    log_level=log_level,
    detailed_summary=True,
  )
  print(result.summary_text)


if __name__ == '__main__':
  cli()
