from neo4j import GraphDatabase
import random
import string
import WalletClustering_neo4jConnect

mihTemplate = '''
MATCH (:Address{address:"%s"})-[:SENDS]->(t:Transaction),
(walletMember:Address)-[:SENDS]->(t:Transaction)
RETURN DISTINCT walletMember
'''

query_update = """CALL apoc.periodic.iterate( 'UNWIND $addresses as item return item',
                        'Match (a:Address {address: item}) set a.association = "%s" return a', 
                        {batchSize:1000, parallel:true, iterateList:true, params:{addresses:%s}})"""

check_association = """Match (a:Address {address: '%s'}) 
where a.association is not null
return true"""


mihWhereList = """MATCH (a:Address)-[:SENDS]->(t:Transaction), (walletMember:Address)-[:SENDS]->(t:Transaction) 
where a.address in %s
RETURN DISTINCT walletMember"""


# Iterating through the addresses and finding all the addresses that are connected to the input address.
# Store all responses in a dictionary and instead of looping over every item and adding only new Addresses to the list,
# write all records of a response into the dictionary. They addresses are the keys
# additionally use a batched version if the amount of retrieved records is greater than 10
def iterMultiInputClustering_chunks(address, setAssociationTo = 'RandomString'):
    
    chunk_size = 500
    # create initial set of addresses
    walletAddresses = {address: 1}
    conn = WalletClustering_neo4jConnect.conn
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
    
    if setAssociationTo == 'RandomString':
        source = string.ascii_letters + string.digits
        walletString = ''.join((random.choice(source) for i in range(32)))
    else:
        walletString = setAssociationTo
    # bevor update prüfe ob es in dem wallet nicht bereits associations gibt
    # -> Beispiel von neue Adresse die zu Binance dazugehört
    # Was passiert mit Wallets die zusammengeführt werden zu einem späteren Zeitpunkt
    # -> Übernimm die erste association 
    # alle Association des Wallets suchen und mit der Blacklist vergleichen; falls es eine Übereinstimmung gibt setze die Ass. der Blacklist
    
    list_of_Addresses = str(list(walletAddresses))
    query_assoc = "MATCH (a:Address)  where a.address in %s RETURN DISTINCT a.association"
    result = conn.query(query_assoc % list_of_Addresses, db='neo4j')
    
    if result[0][0] is not None:
        walletString = result[0][0]
    print("Updating ... "+ str(address) +", with size " + str(len(walletAddresses)) + ": " + walletString) 
    result = conn.query(query_update % (walletString, list_of_Addresses),db='neo4j')
    return walletAddresses, walletString




