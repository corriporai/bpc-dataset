from pathlib import Path
import click
import requests
import hashlib
import pandas as pd
import re
from bs4 import BeautifulSoup
from parfive import Downloader
from itertools import chain


HIGH_IMAGE = "https://botapracorrer.fotop.com.br/fotos/commerceft/download/download-foto-avulsa/a/{}"

URLS = [
    {
        "key": "bpc_jalapao",
        "year": "2019",
        "name": "Olympikus Bota Pra Correr Jalapão",
        "url": "https://botapracorrer.fotop.com.br/fotos/eventos/busca/id/{}/evento/19735/busca/numero",
    },
    {
        "key": "bpc_pantanal",
        "year": "2019",
        "name": "Olympikus Bota Pra Correr Pantanal",
        "url": "https://botapracorrer.fotop.com.br/fotos/eventos/busca/id/{}/evento/22591/busca/numero",
    },
    {
        "key": "bpc_alter_chao",
        "year": "2019",
        "name": "Olympikus Bota Pra Correr Alter do Chao",
        "url": "https://botapracorrer.fotop.com.br/fotos/eventos/busca/id/{}/evento/24747/busca/numero",
    },
    {
        "key": "bpc_veadeiros",
        "year": "2022",
        "name": "Olympikus Bota Pra Correr 2022 - Veadeiros",
        "url": "https://botapracorrer.fotop.com.br/fotos/eventos/busca/id/{}/evento/51646/busca/numero",
    },
    {
        "key": "bpc_milagres",
        "year": "2022",
        "name": "Olympikus Bota Pra Correr 2022 - Milagres",
        "url": "https://botapracorrer.fotop.com.br/fotos/eventos/busca/id/{}/evento/55422/busca/numero",
    },
    {
        "key": "bpc_conde",
        "year": "2022",
        "name": "Bota Pra Correr Costa do Conde",
        "url": "https://botapracorrer.fotop.com.br/fotos/eventos/busca/id/{}/evento/76437/busca/numero",
    },
]


IMAGE_SET = {}


def fetch_image_set(bib: str, event: dict):
    click.echo(
        "Collecting image set for bib {0} from {1} ...".format(bib, event["name"])
    )
    response_data = requests.get(event["url"].format(bib)).text
    html_parsed = BeautifulSoup(response_data, "html.parser")
    all_image_divs = html_parsed.find_all("a", {"class": "fotoCorredor"})
    click.echo(
        "...there are officially {0} records for bib {1}".format(
            len(all_image_divs), bib
        )
    )

    for div in all_image_divs:
        image_pk = re.search(r"id\/(\d+)\/", div["href"]).group(1)
        photo_url = div.find("img")["src"]
        #photo low url, path for cache , photo high url
        photo_path = photo_url.split('/')[-1]
        IMAGE_SET.setdefault(photo_path, (photo_url,None,HIGH_IMAGE.format(image_pk) ))


def load_cache_file(cache_file:pd.DataFrame):
    for index, row in cache_file.iterrows():
        IMAGE_SET.setdefault(str(row['key']), (row['file_low'], row['file_path'] , row['file_high']))

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
    target_dir = target_dir.joinpath(event_dict["key"])
    target_dir.mkdir(parents=True, exist_ok=True)

    for index, row in df_event.iterrows():
        if index < start:
            continue
        if index == stop:
            break
        bib_dir = target_dir.joinpath(str(row["bib"]))
        bib_dir.mkdir(parents=True, exist_ok=True)
        fetch_image_set(str(row["bib"]), event_dict)
        keys = list(IMAGE_SET.keys())
        file_urls = IMAGE_SET.values()
        file_low_urls, file_paths, file_high_urls = map(list,zip(*file_urls))

        not_found_files = [i for i in range(len(file_paths)) if file_paths[i] is None]
        url_to_download_lows = [file_low_urls[idx] for idx in not_found_files]
        files = Downloader.simple_download(list(url_to_download_lows), path=bib_dir)
        for fl in files:
            suffix = fl.split('/')[-1]
            for idx, element in enumerate(file_low_urls):
                if suffix in element:
                    file_paths[idx] =fl
                    IMAGE_SET[keys[idx]] = (IMAGE_SET[keys[idx]][0], fl, IMAGE_SET[keys[idx]][2])




    keys = IMAGE_SET.keys()
    file_urls = IMAGE_SET.values()
    file_low_urls, file_paths, file_high_urls = zip(*file_urls)

    df = pd.DataFrame({"key": keys, "file_path": file_paths, 'file_high': file_high_urls, 'file_low': file_low_urls })
    df.to_csv(
        target_dir.joinpath(event_dict["key"] + "_" + "cache.csv"), index=False
    )

    click.echo("... data saved at {}".format(target_dir))



if __name__ == "__main__":
    cli()
