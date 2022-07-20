# %% [markdown]
# # install packages

# %%
from neo4j import GraphDatabase
import multiprocessing as mp
import pandas as pd
import random
import string
import os
import pickle
import time
from py2neo import Graph 

# %% [markdown]
# # EXECUTE: WalletClustering_neo4jConnect notebook

# %%
#%run ./WalletClustering_neo4jConnect.ipynb # includes Neo4J connector
# methods & variables of notebook can be referenced

session = Graph("neo4j://127.0.0.1:7687", auth=("team", "F0110wTh€M0n€y"))

# %% [markdown]
# # cluster wallets

# %%
mihTemplate = '''
MATCH (:Address{address:"%s"})-[:SENDS]->(t:Transaction),
(walletMember:Address)-[:SENDS]->(t:Transaction)
RETURN DISTINCT walletMember
'''
#address

# %%
# use existing terrorAddressList if exists
if not os.path.exists('output\\terrorAddressList.pickle'):
    createTerrorAddressList()

terrorAddressList = pickle.load(open('output\\terrorAddressList.pickle', 'rb'))
print(terrorAddressList)

# %%
mihWhere = """MATCH (a:Address)-[:SENDS]->(t:Transaction), (walletMember:Address)-[:SENDS]->(t:Transaction) 
where a.address in [\"{0}\",\"{1}\",\"{2}\",\"{3}\",\"{4}\",\"{5}\",\"{6}\",\"{7}\",\"{8}\",\"{9}\"]
RETURN DISTINCT walletMember"""

# %%
def updateWalletAddresses(address, walletName):
  
  query = """CALL apoc.periodic.iterate( 'MATCH (a:Address {address: "%s"})-[:SENDS]->(t:Transaction), (walletMember:Address)-[:SENDS]->(t:Transaction) RETURN  walletMember',
  'set walletMember.association = "%s"', {batchSize:1000, parallel:true})""" % (address, walletName)
  return query


# %%
check_association = """Match (a:Address {address: '%s'}) 
where a.association is not null
return true"""

# %%
# Iterating through the addresses and finding all the addresses that are connected to the input address.
# Store all responses in a dictionary and instead of looping over every item and adding only new Addresses to the list,
# write all records of a response into the dictionary. They addresses are the keys
# additionally use a batched version if the amount of retrieved records is greater than 10
def iterMultiInputClustering_old(address):

    # create initial set of addresses
    walletAddresses = {address: 1}
    response = conn.query(mihTemplate % address, db='neo4j')

    # store every found address as key in the dictionary, values do not matter here, so we just pass 1
    for record in response:
       walletAddresses [record[0]._properties["address"]]= 1 
    
    i = 1
    
    while i < len(walletAddresses):

        # generate a list of the keys to get an index; this is necessary for the batching
        list_ofKeys = list(walletAddresses.keys())
        
        # if there are less than 10 addresses left between i and the maximum; then no batching is possible
        if len(walletAddresses) - i <= 10 :

            response = conn.query(mihTemplate % list_ofKeys[i], db='neo4j')
            
            # this automatically resolves duplicates. Instead of iterating over every address one by one in the list and comparing them with the existing set, 
            # this is more faster since the dictionaries are actually hash tables. So it reaches less than logarithmic runtime
            for record in response:
                walletAddresses [record[0]._properties["address"]]= 1 
            i += 1
            list_ofKeys = list(walletAddresses.keys())
        
        
       # batching 10 addresses at once to avoid querying every single transaction in the dictionary
       # only possible if there are more than 10 addresses left in the dictionary
       # question is if we can further improve this... like with 500 and a function in between that creates a string
        while 10 < len(walletAddresses) - i:

            response = conn.query(mihWhere.format(list_ofKeys[i], list_ofKeys[i+1], list_ofKeys[i+2], list_ofKeys[i+3], list_ofKeys[i+4], list_ofKeys[i+5]
            , list_ofKeys[i+6], list_ofKeys[i+7], list_ofKeys[i+8], list_ofKeys[i+9] ), db='neo4j')

            #same as above
            for record in response:
                walletAddresses [record[0]._properties["address"]]= 1
            i += 10
            list_ofKeys = list(walletAddresses.keys())
    
    source = string.ascii_letters + string.digits
    walletString = ''.join((random.choice(source) for i in range(32)))
    
    print("Updating ... "+ str(address) +", with size " + str(len(walletAddresses)) + ": " + walletString)
    list_of_Addresses = str(list(walletAddresses))
    
    query = """CALL apoc.periodic.iterate( 'UNWIND $addresses as item return item',
                    'Match (a:Address {address: item}) set a.association = "%s" return a', 
                    {batchSize:1000, parallel:true, iterateList:true, params:{addresses:%s}})""" % (walletString, list_of_Addresses)
    result = conn.query(query,db='neo4j')
    print(result)
    return walletAddresses, walletString


# %%
# additional remarks and examples
#print(len(wallets))
#output of 13iQsrwBYdrLpnitG5EV79o3PeHjH8XUBc:  
"""137373
Execution time Query: 3572.6374530792236 seconds
Execution time inWallet: 158.30259037017822 seconds
Execution time IF: 0.022434473037719727 seconds   | Going into statement:  2  times."""
#1EYya5dfNvuYDwpeboGKBtkXzJcEHMCQXR '1PeSDEMzi7nj1ah4YFcgnRmijWpgQqP3Yp' '1P963yWMBFkUouU2Me7cQ6136orZDD4gTf' '1KFiRjjvE4rtheuEYGo9VeDDBvGgmm7nRg' exchanges: Btc38.com-1CELa15H4DMzHtHnuz7LCpSFgFWf61Ra6A, 
# QuadrigaCX.com-1LQF9Suqgm4YtxY6kriiE8DJftNTPTqwAm, CoinHako.com- 3PpSAGEGfA9e995bpCkAFdKaw3fMmo8Eyw, MaiCoin.com - 1Lfktsua4x25UcsqDeuXUrXZq3jSoPpJ1b, Hashnest - 1D7JStLYKJ2ma6yfH7a7DXSom5ZPfyfNM3
#wallets
#no batching: 1EYya5dfNvuYDwpeboGKBtkXzJcEHMCQXR - 7sec, 13iQsrwBYdrLpnitG5EV79o3PeHjH8XUBc - cancelled after 4 hours, 
#batching with 6: 1EYya5dfNvuYDwpeboGKBtkXzJcEHMCQXR - 1.3sec, 13iQsrwBYdrLpnitG5EV79o3PeHjH8XUBc - cancelled after 6 hours

# %%
mihWhereList = """MATCH (a:Address)-[:SENDS]->(t:Transaction), (walletMember:Address)-[:SENDS]->(t:Transaction) 
where a.address in %s
RETURN DISTINCT walletMember"""

# %%
# Iterating through the addresses and finding all the addresses that are connected to the input address.
# Store all responses in a dictionary and instead of looping over every item and adding only new Addresses to the list,
# write all records of a response into the dictionary. They addresses are the keys
# additionally use a batched version if the amount of retrieved records is greater than 10
def iterMultiInputClustering_chunks(address):
    chunk_size = 500
    # create initial set of addresses
    walletAddresses = {address: 1}
    response = conn.query(mihTemplate % address, db='neo4j')

    # store every found address as key in the dictionary, values do not matter here, so we just pass 1
    for record in response:
       walletAddresses [record[0]._properties["address"]]= 1 
    
    i = 1
    
    while i < len(walletAddresses):

        # generate a list of the keys to get an index; this is necessary for the batching
        list_ofKeys = list(walletAddresses.keys())
        
        # if there are less than 10 addresses left between i and the maximum; then no batching is possible
        if len(walletAddresses) - i <= chunk_size :

            response = conn.query(mihTemplate % list_ofKeys[i], db='neo4j')
            
            # this automatically resolves duplicates. Instead of iterating over every address one by one in the list and comparing them with the existing set, 
            # this is more faster since the dictionaries are actually hash tables. So it reaches less than logarithmic runtime
            for record in response:
                walletAddresses [record[0]._properties["address"]]= 1 
            i += 1
            list_ofKeys = list(walletAddresses.keys())
        
        
       # batching 10 addresses at once to avoid querying every single transaction in the dictionary
       # only possible if there are more than 10 addresses left in the dictionary
       # question is if we can further improve this... like with 500 and a function in between that creates a string
        while chunk_size < len(walletAddresses) - i:
            list_ofKeys = list(walletAddresses.keys())
            
            response = conn.query(mihWhereList % str(list_ofKeys[i:i+chunk_size]), db='neo4j')
            #same as above
            for record in response:
                walletAddresses [record[0]._properties["address"]]= 1
            i += chunk_size
            list_ofKeys = list(walletAddresses.keys())
    
    source = string.ascii_letters + string.digits
    walletString = ''.join((random.choice(source) for i in range(32)))
    # bevor update prüfe ob es in dem wallet nicht bereits associations gibt
    # -> Beispiel von neue Adresse die zu Binance dazugehört
    # Was passiert mit Wallets die zusammengeführt werden zu einem späteren Zeitpunkt
    # -> Übernimm die erste association 
    # alle Association des Wallets suchen und mit der Blacklist vergleichen; falls es eine Übereinstimmung gibt setze die Ass. der Blacklist
    
    
    print("Updating ... "+ str(address) +", with size " + str(len(walletAddresses)) + ": " + walletString)
    list_of_Addresses = str(list(walletAddresses))
    
    query = """CALL apoc.periodic.iterate( 'UNWIND $addresses as item return item',
                    'Match (a:Address {address: item}) set a.association = "%s" return a', 
                    {batchSize:1000, parallel:true, iterateList:true, params:{addresses:%s}})""" % (walletString, list_of_Addresses)
    result = conn.query(query,db='neo4j')
    print(result)
    return walletAddresses, walletString


# %%
def clusterAddresses(addressList):
    failing_addresses = []
    for address in addressList:
        response = conn.query(check_association % address,db='neo4j')
        if not response:
            try:
                iterMultiInputClustering_chunks(address)
            except TypeError:
                print(address + " could not be clustered due to internal failure!")
                failing_addresses.append(address)
                continue
        else:
            print(address + ": There is already an association in the DB")
    return  failing_addresses
#terrorAddressList.remove("1QH9hfeSSb2iftcVpgpQp3NsFD174crFoW") -> 2.1 mio entries
#terrorAddressList.remove("3PajPWymUexhewHPczmLQ8CMYatKAGNj3y") -> 34 mio entries
#addresses = ["12sDU3FyYJXc2oRzE6XXuuhVHCBJvaoCC8"]
#failed_addresses = clusterAddresses(addresses)

#addresses = ["1JpSBaUwrZaEgmsYka7mzm9t3Z4syyaw7A"]
#codes = clusterAddresses(addresses)

#print(list(codes.keys()))


