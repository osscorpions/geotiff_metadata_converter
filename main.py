from datetime import datetime
import json
import csv
from typing import Collection, Generator, List
from glob import iglob
import sys

"""Se necesita obtener:
- hora utc (de adquisición)
- día
- mes
- año
- nombre del archivo
- tipo de imagen
- resolución por pixel
- satélite
"""


def parse_json_file(filename: str) -> dict:
    """parse_json_file

    Lee un archivo con nombre de archivo filename
    y devuelve un diccionario de él
    """
    with open(filename, "r") as file:
        return json.load(file)


def parse_json_files(filenames: List[str]):
    """parse_json_files

    Lee una lista de nombres de archivos y devuelve un generador
    de diccionarios con estos archivos
    """
    for filename in filenames:
        yield parse_json_file(filename)


def transform_dict(dictionary: dict):
    """transform_dict

    Mueve el diccionario en el indice 'properties' y lo pasa a la base del diccionario

    No hagas esto en casa (ta feo)
    """
    # TODO 4: Agregar el nombre del archivo
    properties = dictionary["properties"]
    separated_dict = sepatate_date_to_dict(properties["acquired"])
    # TODO 3: Concatenar el source con el tipo de satélite para que quede en el formato solicitado
    satellite = {"satellite": f'{properties["provider"]} {properties["instrument"]}'}
    output_dict = {
        key: value for key, value in dictionary.items() if key != "properties"
    }
    return output_dict | dictionary["properties"] | separated_dict | satellite


def sepatate_date_to_dict(isodate: str) -> dict:
    """sepatate_date_to_dict

    separa un string en formato iso en un diccionario de la forma:

    {
        "time": str,
        "day": int,
        "month": int,
        "year": int,
    }
    """
    # TODO 2: Separar aquired en hora, día, mes y año con el formato solicitado
    date = datetime.fromisoformat(isodate)

    return {
        "time": str(date.time()),
        "day": date.day,
        "month": date.month,
        "year": date.year,
    }


def get_json_filenames(directory: str):
    """get_json_filenames

    Obtiene todos las rutas de los archivos json de una carpeta dada
    """
    return iglob(f"./{directory}/*.json")


def get_dictionary_by_keys(
    keys_to_get: Collection[str] | Generator[str, None, None], dictionary: dict
):
    """get_dictionary_by_keys

    Obtiene un diccionario con únicamente los índices y valores que se encuentren en el
    arreglo (list) de keys_to_get
    """
    return {key: dictionary[key] for key in keys_to_get}


def save_dicts_to_csv(
    filename: str,
    fieldnames: Collection[str],
    dicts: Collection[dict] | Generator[dict, None, None],
):
    with open(filename, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for d in dicts:
            dict_to_save = get_dictionary_by_keys(dictionary=d, keys_to_get=fieldnames)
            writer.writerow(dict_to_save)


if __name__ == "__main__":
    # TODO 5: Agregar nombre de carpeta donde se encuentran los archivos json y nombre de
    # archivo de salida como argumentos del programa "python .\main.py "c:/carpeta_con_jsons/" "c:/carpeta_salida/output.csv"
    n_args = len(sys.argv)
    input_data_dir = "dataset" if n_args < 2 else sys.argv[1]
    output_filename = "output.csv" if n_args < 3 else sys.argv[2]

    json_filenames = get_json_filenames(input_data_dir)
    list_of_dicts = (transform_dict(d) for d in parse_json_files(json_filenames))
    fieldnames = [
        "time",
        "day",
        "month",
        "year",
        "id",
        "pixel_resolution",
        "satellite",
    ]
    save_dicts_to_csv(
        dicts=list_of_dicts, fieldnames=fieldnames, filename=output_filename
    )

    # TODO 1: Obtener información de bandas y tipo de archivo

    # Mini ETL: Extract, Transform, Load
