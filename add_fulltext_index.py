import sys
from kittystore.kittysastore import KittySAStore

store = KittySAStore('postgres://mm3:mm3@localhost/mm3')

listname = sys.argv[1]
store.add_fulltext_indexes(listname)
