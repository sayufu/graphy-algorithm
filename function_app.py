import azure.functions as func
import logging
import json

import requests
import pandas as pd
import networkx as nx
import ast
import matplotlib.pyplot as plt

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


def read_from_http_endpoint(endpoint_url):
    session = requests.Session()
    response = session.get(endpoint_url)

    if response.status_code != 200:
        raise Exception('The request failed with status code {}'.format(response.status_code))

    response_content = response.content
    data_json = json.loads(response_content)

    return data_json


def calculate_weight(node1, node2, productos_df):
    attributes1 = set(productos_df[productos_df['id'] == node1][['category', 'sub_category', 'brand', 'type']].values.flatten())
    attributes2 = set(productos_df[productos_df['id'] == node2][['category', 'sub_category', 'brand', 'type']].values.flatten())

    # Calcula la cantidad de valores iguales entre los conjuntos de atributos
    common_values = len(attributes1.intersection(attributes2))

    return common_values + 1 

def new_Graph(first_node, G):
    if first_node not in G:
        logging.error(f"Node {first_node} not found in the graph.")
        return []
    
    x = list(G[first_node].items())
    graph = []
    for i in x:
        edge = (first_node, i[0], i[1].get("weight"))
        graph.append(edge)
    for i in x:
        newfirst_node = i[0]
        newlist = list(G[newfirst_node].items())
        for j in newlist:
            edge = (newfirst_node, j[0], j[1].get("weight"))
            graph.append(edge)
    return graph

def Prim(G, start_node):
    mst = nx.Graph()
    visited = set([start_node])
    edges = []
    selected = []

    while len(visited) < len(G.nodes):
        min_edge = None

        for node in visited:
            for neighbor, data in G[node].items():
                if neighbor not in visited:
                    edges.append((node, neighbor, data['weight']))

        edges.sort(key=lambda x: x[2],reverse=True)
        for edge in edges:
            node1, node2, weight = edge
            if node1 in visited and node2 not in visited:
                min_edge = edge
                break

        if min_edge:
            node1, node2, weight = min_edge
            visited.add(node2)
            mst.add_edge(node1, node2, weight=weight)
            selected.append(node2)
        edges = []

    return mst, selected

@app.route(route="graphy-recommend")
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request for Graphy App.')

    productId = req.params.get('productId')
    productRecommendations = []
    brandRecommendations = []

    if not productId:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            productId = req_body.get('productId')
    else:
        productos_list = read_from_http_endpoint('https://us-east-1.aws.data.mongodb-api.com/app/graphy-api-cfefb/endpoint/api/products')
        conexiones_list = read_from_http_endpoint('https://us-east-1.aws.data.mongodb-api.com/app/graphy-api-cfefb/endpoint/api/purchases')

        conexiones_df = pd.DataFrame(conexiones_list)
        productos_df = pd.DataFrame(productos_list)

        #productos_df = pd.read_csv('GRAPHY_PRODUCTS.csv')
        #conexiones_df = pd.read_csv('GRAPHY_PURCHASES.csv')

        # Recomendaciones de productos por frecuencia de compra
        G = nx.Graph()
        num_filas = 20 


        for idx, row in conexiones_df.iterrows():
            list_products_str = row['list_products']
    
            if list_products_str:
                productos = list_products_str
                n = len(productos)
        
                if n > 1:
                    for i in range(n - 1):
                        source = productos[i]
                        target = productos[i + 1]
                        weight = calculate_weight(source, target, productos_df)
                        G.add_edge(source, target, weight=weight)
                    source = productos[-1]
                    target = productos[0]
                    weight = calculate_weight(source, target, productos_df)
                    G.add_edge(source, target, weight=weight)
        
        start_node = int(productId)
        nuevo = nx.Graph()
        delimitado = new_Graph(start_node, G)

        for i in delimitado:
            node1, node2, weight = i
            nuevo.add_edge(node1, node2, weight=weight)

        mst, productRecommendations = Prim(nuevo, start_node)

    response = {
        "requestedProduct": int(productId),
        "brandRecommendations": brandRecommendations,
        "productRecommendations": productRecommendations
    }

    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET, POST, PUT, DELETE, OPTIONS",
        "Access-Control-Allow-Headers": "Content-Type, Authorization",
    }

    if productId:
        return func.HttpResponse(json.dumps(response), mimetype="application/json", status_code=200, headers=headers)
    else:
        return func.HttpResponse(
            "This HTTP triggered function executed successfully. Pass a product id to get product recommendations with format '/?productId=7'",
            status_code=200,
            headers=headers
        )