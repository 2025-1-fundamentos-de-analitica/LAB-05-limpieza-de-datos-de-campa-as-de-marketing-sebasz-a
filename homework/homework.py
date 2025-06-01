"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel

import glob
import os
import zipfile
import pandas as pd


def load_data(directory):
    """
    Carga el directorio ingresado y devuelve un dataframe con los datos
    """
    data = []

    for zip_file in glob.glob(directory + '/*.zip'):
        with zipfile.ZipFile(zip_file) as z:
            with z.open(z.namelist()[0]) as file:
                data.append(pd.read_csv(file))
    return pd.concat(data, ignore_index=True)

def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortgage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaign_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - cons_price_idx
    - euribor_three_months



    """

    # cargar los datos y eliminar la columna innecesaria
    data = load_data("files/input")
    data.pop("Unnamed: 0")

    #limpiar los datos
    data["job"] = data["job"].str.replace('.', '').str.replace('-', '_')
    data["education"] = data["education"].str.replace(".", "_").replace("unknown", pd.NA)
    data["credit_default"] = data["credit_default"].map(lambda x: 1 if x == "yes" else 0)
    data["mortgage"] = data["mortgage"].map(lambda x: 1 if x == "yes" else 0)

    data["previous_outcome"] = data["previous_outcome"].map(lambda x: 1 if x == "success" else 0)
    data["campaign_outcome"] = data["campaign_outcome"].map(lambda x: 1 if x == "yes" else 0)
    months = {
        "jan": "01",
        "feb": "02",
        "mar": "03",
        "apr": "04",
        "may": "05",
        "jun": "06",
        "jul": "07",
        "aug": "08",
        "sep": "09",
        "oct": "10",
        "nov": "11",
        "dec": "12",
    }
    days = {
        1: "01",
        2: "02",
        3: "03",
        4: "04",
        5: "05",
        6: "06",
        7: "07",
        8: "08",
        9: "09",
    }
    data["month"] = data["month"].map(lambda x: months[x])
    data["day"] = data["day"].map(lambda x: days[x] if x in days else str(x))
    data["last_contact_date"] = data.apply(lambda row: f"2022-{row['month']}-{row['day']}", axis=1)

    # crear los subconjuntos para cada dataset
    client = data[["client_id", "age", "job", "marital", "education", "credit_default", "mortgage"]]
    campaign = data[["client_id", "number_contacts", "contact_duration", "previous_campaign_contacts",
                     "previous_outcome", "campaign_outcome", "last_contact_date"]]
    economics = data[["client_id", "cons_price_idx", "euribor_three_months"]]

    # guardar los dataframes al csv
    if not os.path.exists("files/output"):
        os.mkdir("files/output")
    client.to_csv("files/output/client.csv", index=False)
    campaign.to_csv("files/output/campaign.csv", index=False)
    economics.to_csv("files/output/economics.csv", index=False)

if __name__ == "__main__":
    clean_campaign_data()
