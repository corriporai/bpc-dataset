from pathlib import Path
import click
import requests
import hashlib
import pandas as pd
import re
from bs4 import BeautifulSoup
from parfive import Downloader
from itertools import islice


URLS = [
    {
        "key": "bpc_jalapao",
        "year": "2019",
        "name": "Olympikus Bota Pra Correr Jalapão",
    },
    {
        "key": "bpc_pantanal",
        "year": "2019",
        "name": "Olympikus Bota Pra Correr Pantanal",
    },
    {
        "key": "bpc_alter_chao",
        "year": "2019",
        "name": "Olympikus Bota Pra Correr Alter do Chao",
    },
    {
        "key": "bpc_veadeiros",
        "year": "2022",
        "name": "Olympikus Bota Pra Correr 2022 - Veadeiros",
    },
    {
        "key": "bpc_milagres",
        "year": "2022",
        "name": "Olympikus Bota Pra Correr 2022 - Milagres",
    },
    {
        "key": "bpc_conde",
        "year": "2022",
        "name": "Bota Pra Correr Costa do Conde",
    },
]


IMAGE_SET = {}


def load_cache_file(cache_file:pd.DataFrame):
    for index, row in cache_file.iterrows():
        IMAGE_SET.setdefault(str(row['key']), (row['file_path'], row['bib'] , row['file_high']))

def batched(iterable, n):
    # batched('ABCDEFG', 3) --> ABC DEF G
    if n < 1:
        raise ValueError('n must be at least one')
    it = iter(iterable)
    while batch := tuple(islice(it, n)):
        yield batch

@click.command()
@click.argument("filepath", type=click.Path(exists=True))
@click.argument("targetpath", type=click.Path(exists=True), default=".")
@click.argument("cachepath", required=False)
@click.option('--start', default = '0')
@click.option('--stop', default='ALL')
def cli(filepath: str, targetpath: str, cachepath:str, start:str, stop:str):
    source_file = Path(filepath)
    target_dir = Path(targetpath)

    if stop !='ALL':
        stop = int(stop)

    start=  int(start)

    if cachepath:
        cachepath = Path(cachepath)
        if not cachepath.exists():
            click.echo("The cache file doesn't exist")
            raise SystemExit(1)

        load_cache_file(pd.read_csv(cachepath))

    if not target_dir.exists():
        click.echo("The target directory doesn't exist")
        raise SystemExit(1)

    if not source_file.exists():
        click.echo("The source file doesn't exist")
        raise SystemExit(1)


    selected_event = None
    event = filepath
    for event_dict in URLS:
        if event_dict["key"] in event:
            selected_event = event_dict
            break
    else:
        click.echo("The event does not exist: %s" % event)
        raise SystemExit(1)

    df_event = pd.read_csv(source_file)
    df_event['bib'] = df_event['file_path'].str.split("\/").str[1]

    target_dir = target_dir.joinpath(event_dict["key"])
    target_dir.mkdir(parents=True, exist_ok=True)

    df_event = df_event.groupby('bib').agg({'file_high':lambda x: list(x)})

    for index, (key, group) in enumerate(df_event.iterrows()):
        if index < start:
            continue
        if index == stop:
            break

        bib_dir = target_dir.joinpath(str(key))
        bib_dir.mkdir(parents=True, exist_ok=True)

        click.echo(
        "Collecting image set for bib {0} from {1} images ...".format(key, len(group['file_high'])))
        data = (group['file_high'])
        urls_to_download = [key_url for key_url in data if key_url.split('/')[-1] not in IMAGE_SET]
        click.echo('Only {0} will be downloaded. Already downloaded {1}'.format(len(urls_to_download), len(data) - len(urls_to_download)))

        for to_download in batched(urls_to_download, 10):
            files = Downloader.simple_download(list(to_download), path=bib_dir)
            for file_path, url in zip(files, to_download):
                suffix = file_path.split('/')[-1]
                key_url = url.split('/')[-1]
                IMAGE_SET[key_url] = (file_path, key, url)

    keys = IMAGE_SET.keys()
    file_urls = IMAGE_SET.values()
    file_paths, bibs, file_high_urls = zip(*file_urls)

    df = pd.DataFrame({"key": keys, "file_path": file_paths, 'bib': bibs, 'file_high': file_high_urls })
    df.to_csv(
        target_dir.joinpath(event_dict["key"] + "_high_" + "cache.csv"), index=False
    )

    click.echo("... data saved at {}".format(target_dir))

if __name__ == "__main__":
    cli()
