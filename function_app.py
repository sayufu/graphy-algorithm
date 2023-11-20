

import azure.functions as func
import logging
import json

import requests
import pandas as pd
import networkx as nx
import time

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

cached_data = {}
cached_graph = None

def read_from_http_endpoint(endpoint_url):
    # Check if data is already in the cache
    if endpoint_url in cached_data:
        return cached_data[endpoint_url]

    session = requests.Session()
    response = session.get(endpoint_url)

    if response.status_code != 200:
        raise Exception(f'The request to {endpoint_url} failed with status code {response.status_code}')

    response_content = response.content
    data_json = json.loads(response_content)

    # Cache the data for future use
    cached_data[endpoint_url] = data_json

    return data_json


def calculate_weight(node1, node2, productos_df):
    attributes1 = set(productos_df.loc[productos_df['id'] == node1, ['category', 'sub_category', 'brand', 'type']].values.flatten())
    attributes2 = set(productos_df.loc[productos_df['id'] == node2, ['category', 'sub_category', 'brand', 'type']].values.flatten())

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

def gen_edge(Graph,productos, productos_df):
    n = len(productos)

    if n > 1:
        for i in range(n - 1):
            source = productos[i]
            target = productos[i + 1]
            weight = calculate_weight(source, target, productos_df)
            Graph.add_edge(source, target, weight=weight)
        source = productos[-1]
        target = productos[0]
        weight = calculate_weight(source, target, productos_df)
        Graph.add_edge(source, target, weight=weight)

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
            selected.append(node2)
        edges = []

    return mst, selected


@app.route(route="graphy-recommend")
def http_trigger(req: func.HttpRequest) -> func.HttpResponse:
    global cached_graph
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

        conexiones_df = pd.DataFrame(conexiones_list)[['ID','list_products']]
        productos_df = pd.DataFrame(productos_list)[['id','category', 'sub_category', 'brand', 'type']]


        if cached_graph is None:
            G = nx.Graph()
            start_time = time.time()
            for idx, row in conexiones_df.iterrows():
                list_products_str = row['list_products']

                if list_products_str:
                    productos = list_products_str
                    gen_edge(G, productos, productos_df)

            end_time = time.time()
            logging.info(f'Time taken by building the graph: {end_time - start_time} seconds')

            cached_graph = G


        start_node = int(productId)
        G = cached_graph
        delimitado = new_Graph(start_node, G)


        # Recomendaciones por producto
        nuevo = nx.Graph()
        for i in delimitado:
            node1, node2, weight = i
            nuevo.add_edge(node1, node2, weight=weight)

        mst, productRecommendations = Prim(nuevo, start_node)


        # Recomendaciones por marca
        marca_seleccionada = productos_df.loc[productos_df['id'] == start_node, 'brand'].values[0]
        productos_misma_marca = productos_df[productos_df['brand'] == marca_seleccionada]
        ids_productos_misma_marca = productos_misma_marca['id'].convert_dtypes(int).tolist()
        marcas = nx.Graph()
        gen_edge(marcas,ids_productos_misma_marca, productos_df)
        brandRecommendations = ids_productos_misma_marca

    response = {
        "requestedProduct": int(productId),
        "brandRecommendations": brandRecommendations[:20],
        "productRecommendations": productRecommendations[:20]
    }

    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET",
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
    

@app.route(route="graphy-purchase")
def add_purchases(req: func.HttpRequest) -> func.HttpResponse:
    global cached_graph
    logging.info('Python HTTP trigger function processed a request for Graphy App.')

    conexiones_list = read_from_http_endpoint('https://us-east-1.aws.data.mongodb-api.com/app/graphy-api-cfefb/endpoint/api/purchases')
    productos_list = read_from_http_endpoint('https://us-east-1.aws.data.mongodb-api.com/app/graphy-api-cfefb/endpoint/api/products')

    productos_df = pd.DataFrame(productos_list)[['id','category', 'sub_category', 'brand', 'type']]
    last_record = None

    if conexiones_list:
        last_record = conexiones_list[-1]
        print("Last record:", last_record)
    else:
        print("The list is empty.")


    list_products_str = last_record['list_products']

    if list_products_str:
        productos = list_products_str
        if cached_graph is None:
            return func.HttpResponse(
                f"No existing cached graph",
                status_code=500
            )
        else:
            gen_edge(cached_graph, productos, productos_df)
            return func.HttpResponse(
                f"Added the purchase with products {productos} to the graph.",
                status_code=200
            )


    headers = {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Methods": "GET",
        "Access-Control-Allow-Headers": "Content-Type, Authorization",
    }

    return func.HttpResponse(
        "This HTTP is to trigger to add purchases to an existing graph",
        status_code=200,
        headers=headers
    )

