# -*- coding: utf-8 -*-

import datetime
from pprint import pprint
import time
from kittystore.kittysastore import KittySAStore
from kittystore.mongostore import KittyMGStore

# Define global constant

TABLE = 'devel'
REP = 30
URL = 'postgres://mm3:mm3@localhost/mm3'


def db_store_factory():
    return KittySAStore(URL)


def mg_store_factory():
    return KittyMGStore(host='localhost', port=27017)

START = datetime.datetime(2012, 3, 1)
END = datetime.datetime(2012, 3, 30)

# store the results in a global variable
results = {}


def row(values):
    return '\t'.join(['%s' % v for v in values]) + '\n'


def output(testname, results):
    keys = sorted(results.keys())
    value_lists = [results[key] for key in keys]
    if len(set([len(value_list) for value_list in value_lists])) != 1:
        print 'Something went wrong. No output.'
        return
    stream = open(testname, 'w')
    stream.write(row(keys))

    # skip the first value in the round
    for zipped_values in zip(*value_lists)[1:]:
        stream.write(row(zipped_values))
    stream.close()


def run(testname, variant, rep, factory, funcname, *args, **kwargs):
    print variant
    testresults = results.setdefault(testname, {})
    if variant in testresults:
        print 'error, key already in results'
        pprint(testresults)
        return
    testresults[variant] = []
    retval = None
    for i in range(0, rep):
        store = factory()
        start = time.time()
        try:
            retval = getattr(store, funcname)(*args, **kwargs)
        except NotImplementedError:
            del testresults[variant]
            raise
        testresults[variant].append(time.time() - start)
        if hasattr(store, 'engine'):
            # cleanup sqlalchemy connections
            store.engine.dispose()
            del store.engine
        del store
    return retval


def hashable(value):
    if isinstance(value, dict):
        val = []
        for (key, value) in value.items():
            val.append((hashable(key), hashable(value)))
        return tuple(val)
    elif isinstance(value, list):
        valuelist = [hashable(v) for v in value]
        return tuple(valuelist)
    else:
        return value


def run_tests(testname, rep, tests, *args, **kwargs):
    print testname
    test_key_func = kwargs.pop('test_key_func', None)
    retvals = []
    for (variant, factory, funcname) in tests:
        try:
            retval = run(testname, variant, rep, factory, funcname,
                         *args, **kwargs)
        except NotImplementedError:
            print 'Skipped.'
            continue
        if test_key_func is not None:
            retvals.append(hashable(test_key_func(retval)))
        else:
            retvals.append(retval)
    output(testname, results[testname])

    if test_key_func is None:
        return retvals

    names = [test[0] for test in tests]
    if len(set(retvals)) != 1:
        print '** Results differs'
        for (name, retval) in zip(names, retvals):
            print '{name}: {retval}'.format(name=name,
                                            retval=retval)


def get_email(rep):
    (res_pg, res_mg) = run_tests('get_email', rep,
                                 [['PG', db_store_factory, 'get_email'],
                                  ['MG', mg_store_factory, 'get_email']],
                                 TABLE, '3D97B04F.7090405@terra.com.br',
                                 test_key_func=None)
    if res_mg['Subject'] != res_pg.subject and res_mg['Date'] != res_pg.date:
        print '** Results differs'
        print 'MG: %s' % res_mg
        print 'PG: %s\n' % res_pg


def get_archives_range(rep):
    run_tests('get_archives_range', rep,
              [['PG', db_store_factory, 'get_archives'],
               ['MG', mg_store_factory, 'get_archives']],
              TABLE, START, END,
              test_key_func=len)


def first_email_in_archives_range(rep):
    (res_pg, res_mg) = run_tests('first_email_in_archives_range', rep,
                                 [['PG', db_store_factory, 'get_archives'],
                                  ['MG', mg_store_factory, 'get_archives']],
                                 TABLE, START, END)
    res_pg = res_pg[0]
    res_mg = res_mg[0]
    if res_mg['Subject'] != res_pg.subject and res_mg['Date'] != res_pg.date:
        print '** Results differs'
        print 'MG: %s' % res_mg
        print 'PG: %s\n' % res_pg


def get_thread_length(rep):
    run_tests('get_thread_length', rep,
              [['PG', db_store_factory, 'get_thread_length'],
               ['MG', mg_store_factory, 'get_thread_length']],
              TABLE, '4FCWUV6BCP3A5PASNFX6L5JOAE4GJ7F2',
              test_key_func=lambda x: x)


def get_thread_participants(rep):
    run_tests('get_thread_participants', rep,
              [['PG', db_store_factory, 'get_thread_participants'],
               ['MG', mg_store_factory, 'get_thread_participants']],
              TABLE, '4FCWUV6BCP3A5PASNFX6L5JOAE4GJ7F2',
              test_key_func=len)


def get_archives_length(rep):
    run_tests('get_archives_length', rep,
              [['PG', db_store_factory, 'get_archives_length'],
               ['MG', mg_store_factory, 'get_archives_length']], TABLE,
              test_key_func=lambda x: x)


def search_subject(rep):
    run_tests('search_subject', rep,
              [['PG-CS', db_store_factory, 'search_subject'],
               ['PG-IN', db_store_factory, 'search_subject_index'],
               ['MG-CS', mg_store_factory, 'search_subject']],
              TABLE, 'rawhid',
              test_key_func=len)


def search_subject_cs(rep):
    run_tests('search_subject_cs', rep,
              [['PG-CS', db_store_factory, 'search_subject_cs'],
               ['MG-CS', mg_store_factory, 'search_subject_cs']],
              TABLE, 'rawhid',
              test_key_func=len)


def search_content(rep):
    run_tests('search_content', rep,
              [['PG', db_store_factory, 'search_content'],
               ['PG-IN', db_store_factory, 'search_content_index'],
               ['MG', mg_store_factory, 'search_content']], TABLE, 'rawhid',
              test_key_func=len)


def search_content_cs(rep):
    run_tests('search_content_cs', rep,
              [['PG', db_store_factory, 'search_content_cs'],
               ['MG', mg_store_factory, 'search_content_cs']], TABLE, 'rawhid',
              test_key_func=len)


def search_content_subject(rep):
    run_tests('search_content_subject', rep,
              [['PG', db_store_factory, 'search_content_subject'],
               ['PG-OR', db_store_factory, 'search_content_subject_or'],
               ['PG-IN', db_store_factory, 'search_content_subject_index'],
               ['MG', mg_store_factory, 'search_content_subject']],
               TABLE, 'rawhid', test_key_func=len)


def search_content_subject_300_30(rep):
    run_tests('search_content_subject_300_30', rep,
              [['PG', db_store_factory, 'search_content_subject'],
               ['PG-OR', db_store_factory, 'search_content_subject_or'],
               ['PG-IN', db_store_factory, 'search_content_subject_index'],
               ['MG', mg_store_factory, 'search_content_subject']],
               TABLE, 'rawhid', limit=30, offset=300, test_key_func=len)


def search_content_subject_5000_30(rep):
    run_tests('search_content_subject_5000_30', rep,
              [['PG', db_store_factory, 'search_content_subject'],
               ['PG-OR', db_store_factory, 'search_content_subject_or'],
               ['PG-IN', db_store_factory, 'search_content_subject_index'],
               ['MG', mg_store_factory, 'search_content_subject']],
               TABLE, 'rawhid', limit=30, offset=5000, test_key_func=len)


def search_content_subject_cs(rep):
    run_tests('search_content_subject_cs', rep,
              [['PG-CS', db_store_factory, 'search_content_subject_cs'],
               ['PG-OR-CS', db_store_factory, 'search_content_subject_or_cs'],
               ['MG-CS', mg_store_factory, 'search_content_subject_cs']],
               TABLE, 'rawhid', test_key_func=len)


def search_sender(rep):
    run_tests('search_sender', rep,
              [['PG', db_store_factory, 'search_sender'],
               ['PG-OR', db_store_factory, 'search_sender_or'],
               ['MG', mg_store_factory, 'search_sender']],
              TABLE, 'rawhid', test_key_func=len)


def search_sender_cs(rep):
    run_tests('search_sender_cs', rep,
              [['PG', db_store_factory, 'search_sender_cs'],
               ['PG-OR', db_store_factory, 'search_sender_or_cs'],
               ['MG', mg_store_factory, 'search_sender_cs']],
              TABLE, 'rawhid', test_key_func=len)


def get_list_size(rep):
    run_tests('get_list_size', rep,
              [['PG', db_store_factory, 'get_list_size'],
               ['MG', mg_store_factory, 'get_list_size']],
              TABLE, test_key_func=lambda x: x)

if __name__ == '__main__':
    t_start = time.time()
    get_email(REP)
    get_archives_range(REP)
    first_email_in_archives_range(REP)
    get_thread_length(REP)
    get_thread_participants(REP)
    get_archives_length(REP)
    search_subject(REP)
    search_subject_cs(REP)
    search_content(REP)
    search_content_cs(REP)
    search_content_subject(REP)
    search_content_subject_300_30(REP)
    search_content_subject_5000_30(REP)
    search_content_subject_cs(REP)
    search_sender(REP)
    search_sender_cs(REP)
    get_list_size(REP)
    print "Ran for %s seconds" % (time.time() - t_start)
