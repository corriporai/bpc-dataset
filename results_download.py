from pathlib import Path
import click
import requests
import hashlib
import pandas as pd


URLS = [
    {
        "key": "bpc_jalapao",
        "year": "2019",
        "name": "Olympikus Bota Pra Correr Jalapão",
        "url": "https://polesportivo.com.br/controllers/resultado.php?id=391&_=1700047992680",
    },
    {
        "key": "bpc_pantanal",
        "year": "2019",
        "name": "Olympikus Bota Pra Correr Pantanal",
        "url": "https://polesportivo.com.br/controllers/resultado.php?id=443&_=1700048123911",
    },
    {
        "key": "bpc_alter_chao",
        "year": "2019",
        "name": "Olympikus Bota Pra Correr Alter do Chao",
        "url": "https://polesportivo.com.br/controllers/resultado.php?id=504&_=1700048297225",
    },
    {
        "key": "bpc_veadeiros",
        "year": "2022",
        "name": "Olympikus Bota Pra Correr 2022 - Veadeiros",
        "url": "https://polesportivo.com.br/controllers/resultado.php?id=775&_=1700048335787",
    },
    {
        "key": "bpc_milagres",
        "year": "2022",
        "name": "Olympikus Bota Pra Correr 2022 - Milagres",
        "url": "https://polesportivo.com.br/controllers/resultado.php?id=813&_=1700048400884",
    },
    {
        "key": "bpc_conde",
        "year": "2022",
        "name": "Bota Pra Correr Costa do Conde",
        "url": "https://polesportivo.com.br/controllers/resultado.php?id=948&_=1700048460793",
    },
]

keys = [ 
    'race',
    'category', 
    'bib', 
    'name', 
    'team', 
    'sex', 
    'age', 
    'id', 
    'place', 
    'catPlace', 
    'sexPlace',
    'netTime', 
]

CACHE_IDS = {}

def clean_data(record: dict, key:str):
    clean_record = {}
    try:
        clean_record['race'] = record['prova']
    except KeyError:
        if key in ('bpc_jalapao', 'bpc_pantanal', 'bpc_alter_chao'):
            clean_record['race'] = '10KM' if '10KM' in record['categoria'] else '21KM'
        else:
            clean_record['race'] = record['prova']
    clean_record['category'] = record['categoria']
    clean_record['bib'] = record['numero']
    clean_record['name'] = hashlib.sha1(record['nome'].encode()).hexdigest()

    try:
        clean_record['id'] = hashlib.sha1(record['cpf'].encode()).hexdigest()
    except AttributeError:
        clean_record['id'] = CACHE_IDS.get(clean_record['name'], None)

    CACHE_IDS[clean_record['name']] = clean_record['id']

    try:
        clean_record['place'] = int(record['rank_geral'])
    except ValueError:
        clean_record['place'] = (record['rank_geral'])
    try:
        clean_record['catPlace'] = int(record['rank_categoria'])
    except ValueError:
        clean_record['catPlace'] = (record['rank_categoria'])

    try:
        clean_record['sexPlace'] = int(record['rank_sexo'])
    except ValueError:
        clean_record['sexPlace'] = (record['rank_sexo'])

    clean_record['sex'] = (record['sexo'])
    clean_record['age'] = (record['idade'])

    clean_record['team'] = record['equipe']
    clean_record['netTime'] = record['tempo_total']

    return clean_record

def fetch_results(row: dict):
    click.echo("Collecting data for event {}...".format(row['name']))
    json_data = requests.get(row['url']).json()
    result_set = []
    for record in json_data['data']:
        if 'nao identificado' not in record['categoria']:
            parsed_record = clean_data(record, row['key'])
            result_set.append(parsed_record)

    click.echo("...there are officially {0} records for event {1}".format(len(result_set), row['name']))

    return result_set

@click.command()
@click.argument("event", default="ALL")
@click.argument("filepath", type=click.Path(exists=True), default=".")
def cli(event: str, filepath: str):
    target_dir = Path(filepath)
    if not target_dir.exists():
        click.echo("The target directory doesn't exist")
        raise SystemExit(1)
    selected_event = URLS
    for event_dict in URLS:
        if event == event_dict["key"]:
            selected_event = [event_dict]
            break
    else:
        if event != "ALL":
            click.echo("The event does not exist: %s" % event)
            raise SystemExit(1)

    for row in selected_event:
        result_set = fetch_results(row)
        df = pd.DataFrame(result_set)
        target_path = target_dir.joinpath('{0}_{1}.csv'.format(row['key'], row['year']))
        df.to_csv( target_path, index=False)

    click.echo('... data saved at {}'.format(target_path))


if __name__ == "__main__":
    cli()
