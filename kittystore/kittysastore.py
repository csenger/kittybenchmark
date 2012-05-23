# -*- coding: utf-8 -*-

"""
KittySAStore - an object mapper and interface to a SQL database
           representation of emails for mailman 3.

Copyright (C) 2012 Pierre-Yves Chibon
Author: Pierre-Yves Chibon <pingou@pingoured.fr>

This program is free software; you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation; either version 2 of the License, or (at
your option) any later version.
See http://www.gnu.org/copyleft/gpl.html  for the full text of the
license.
"""

import datetime
import time

from kittystore import KittyStore
from kittystore.kittysamodel import get_class_object


from sqlalchemy import create_engine, distinct, MetaData, and_, desc, or_
from sqlalchemy.exc import ProgrammingError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy import event
from sqlalchemy.engine import Engine

import logging

logging.basicConfig()
logger = logging.getLogger("Postgres")
logger.setLevel(logging.DEBUG)


#@event.listens_for(Engine, "before_cursor_execute")
def before_cursor_execute(conn, cursor, statement,
                        parameters, context, executemany):
    context._query_start_time = time.time()
    logger.debug("Start Query:\n%s" % statement)
    logger.debug("Parameters:\n%r" % (parameters,))


#@event.listens_for(Engine, "after_cursor_execute")
def after_cursor_execute(conn, cursor, statement,
                        parameters, context, executemany):
    total = time.time() - context._query_start_time
    logger.debug("Query Complete!")
    logger.debug("Total Time: %.02fms" % (total * 1000))


def list_to_table_name(list_name):
    """ For a given fully qualified list name, return the table name.
    What the method does is to transform the special characters from the
    list name to underscore ('_') and append the 'KS_' prefix in front.
    (KS stands for KittyStore).

    Characters replaced: -.@

    :arg list_name, the fully qualified list name to be transformed to
    the table name.
    """
    for char in ['-', '.', '@']:
        list_name = list_name.replace(char, '_')
    return 'HK_%s' % list_name


class KittySAStore(KittyStore):
    """ SQL-Alchemy powered interface to query emails from the database.
    """

    def __init__(self, url, debug=False):
        """ Constructor.
        Create the session using the engine defined in the url.

        :arg url, URL used to connect to the database. The URL contains
        information with regards to the database engine, the host to connect
        to, the user and password and the database name.
          ie: <engine>://<user>:<password>@<host>/<dbname>
          ie: mysql://mm3_user:mm3_password@localhost/mm3
        :kwarg debug, a boolean to set the debug mode on or off.
        """
        self.engine = create_engine(url, echo=debug)
        self.metadata = MetaData(self.engine)
        session = sessionmaker(bind=self.engine)
        self.session = session()

    def add_fulltext_indexes(self, list_name):
        '''
        Create a full text index for a list table
        '''

        table_name = list_to_table_name(list_name)

        def execute(sql):
            '''
            print, execute, log error if any and pass.
            '''
            print '-' * 60
            print 'Statement: ', sql[:60]
            try:
                self.engine.execute(sql)
            except ProgrammingError, exception:
                if exception.orig.pgcode in ['42710', '42P07']:
                    print 'exists.'
                else:
                    print 'failed: %s\nstatement:%s' % (exception.orig.pgerror,
                                                        sql)
            else:
                print 'done.'

        for columns in [['content'], ['subject'], ['content', 'subject']]:
            index_name = "%s_fulltext_index" % ('_'.join(columns))

            # Add indexes for different queries
            execute(('CREATE INDEX "%s" ON "%s" USING '
                     "gin(to_tsvector('english', %s))") %
                     (index_name, table_name, " || ' ' || ".join(columns)))

            ## Alternatively: a column based solution outlined in
            ## http://www.postgresql.org/docs/9.1/interactive/textsearch-tables.html#TEXTSEARCH-TABLES-INDEX

    def get_archives(self, list_name, start, end):
        """ Return all the thread started emails between two given dates.

        :arg list_name, name of the mailing list in which this email
        should be searched.
        :arg start, a datetime object representing the starting date of
        the interval to query.
        :arg end, a datetime object representing the ending date of
        the interval to query.
        """
        # Beginning of thread == No 'References' header
        email = get_class_object(list_to_table_name(list_name), 'email',
            self.metadata)
        mails = self.session.query(email).filter(
            and_(
                email.date >= start,
                email.date <= end,
                email.references == None)
                ).order_by(email.date).all()
        mails.reverse()
        return mails

    def get_archives_length(self, list_name):
        """ Return a dictionnary of years, months for which there are
        potentially archives available for a given list (based on the
        oldest post on the list).

        :arg list_name, name of the mailing list in which this email
        should be searched.
        """
        archives = {}
        email = get_class_object(list_to_table_name(list_name), 'email',
            self.metadata)
        entry = self.session.query(email).order_by(
                    email.date).limit(1).all()[0]
        now = datetime.datetime.now()
        year = entry.date.year
        month = entry.date.month
        while year < now.year:
            archives[year] = range(1, 13)[(month -1):]
            year = year + 1
            month = 1
        archives[now.year] = range(1, 13)[:now.month]
        return archives

    def get_email(self, list_name, message_id):
        """ Return an Email object found in the database corresponding
        to the Message-ID provided.

        :arg list_name, name of the mailing list in which this email
        should be searched.
        :arg message_id, Message-ID as found in the headers of the email.
        Used here to uniquely identify the email present in the database.
        """
        email = get_class_object(list_to_table_name(list_name), 'email',
            self.metadata)
        mail = None
        try:
            mail = self.session.query(email).filter_by(
                message_id=message_id).one()
        except NoResultFound:
            pass
        return mail

    def get_list_size(self, list_name):
        """ Return the number of emails stored for a given mailing list.

        :arg list_name, name of the mailing list in which this email
        should be searched.
        """
        email = get_class_object(list_to_table_name(list_name), 'email',
            self.metadata)
        return self.session.query(email).count()

    def get_thread(self, list_name, thread_id):
        """ Return all the emails present in a thread. This thread
        is uniquely identified by its thread_id.

        :arg list_name, name of the mailing list in which this email
        should be searched.
        :arg thread_id, thread_id as used in the web-pages.
        Used here to uniquely identify the thread in the database.
        """
        email = get_class_object(list_to_table_name(list_name), 'email',
            self.metadata)
        mail = None
        try:
            mail = self.session.query(email).filter_by(
                thread_id=thread_id).order_by(email.date).all()
        except NoResultFound:
            pass
        return mail

    def get_thread_length(self, list_name, thread_id):
        """ Return the number of email present in a thread. This thread
        is uniquely identified by its thread_id.

        :arg list_name, name of the mailing list in which this email
        should be searched.
        :arg thread_id, unique identifier of the thread as specified in
        the database.
        """
        email = get_class_object(list_to_table_name(list_name), 'email',
            self.metadata)
        return self.session.query(email).filter_by(
                    thread_id=thread_id).count()

    def get_thread_participants(self, list_name, thread_id):
        """ Return the list of participant in a thread. This thread
        is uniquely identified by its thread_id.

        :arg list_name, name of the mailing list in which this email
        should be searched.
        :arg thread_id, unique identifier of the thread as specified in
        the database.
        """
        email = get_class_object(list_to_table_name(list_name), 'email',
            self.metadata)
        return self.session.query(distinct(email.sender)).filter(
                email.thread_id == thread_id).all()

    def search_content(self, list_name, keyword):
        """ Returns a list of email containing the specified keyword in
        their content.

        :arg list_name, name of the mailing list in which this email
        should be searched.
        :arg keyword, keyword to search in the content of the emails.
        """
        email = get_class_object(list_to_table_name(list_name), 'email',
            self.metadata)
        mails = self.session.query(email).filter(
                email.content.ilike('%{0}%'.format(keyword))
                ).order_by(email.date).all()
        mails.reverse()
        return mails

    def search_content_cs(self, list_name, keyword):
        """ Returns a list of email containing the specified keyword in
        their content.

        :arg list_name, name of the mailing list in which this email
        should be searched.
        :arg keyword, keyword to search in the content of the emails.
        """
        email = get_class_object(list_to_table_name(list_name), 'email',
            self.metadata)
        mails = self.session.query(email).filter(
                email.content.like('%{0}%'.format(keyword))
                ).order_by(email.date).all()
        mails.reverse()
        return mails

    def search_content_index(self, list_name, keyword, limit=None,
                             offset=None):
        email = get_class_object(list_to_table_name(list_name), 'email',
                                 self.metadata)
        criterion = "to_tsvector('english', content) @@ to_tsquery(:keyword)"
        keyword = '%s:*' % keyword
        q = self.session.query(email).order_by(email.date)
        q = q.filter(criterion).params(keyword=keyword)
        if limit is not None:
            # imply that the result set is that big
            q = q.offset(None).limit(limit)
        all = q.all()
        return all

    def search_subject_index(self, list_name, keyword, limit=None, offset=300):
        email = get_class_object(list_to_table_name(list_name), 'email',
                                 self.metadata)
        criterion = "to_tsvector('english', subject) @@ to_tsquery(:keyword)"
        keyword = '%s:*' % keyword

        q = self.session.query(email)
        q = q.filter(criterion).params(keyword=keyword).order_by(email.date)
        if limit is not None:
            # imply that the result set is that big
            q = q.offset(offset).limit(limit)
        return q.all()

    def search_content_subject(self, list_name, keyword, limit=None,
                               offset=None):
        """ Returns a list of email containing the specified keyword in
        their content or their subject.

        :arg list_name, name of the mailing list in which this email
        should be searched.
        :arg keyword, keyword to search in the content or subject of
        the emails.
        """
        if limit is not None:
            # not implemented, skip the result
            raise NotImplementedError
        email = get_class_object(list_to_table_name(list_name), 'email',
            self.metadata)
        mails = self.session.query(email).filter(
                email.content.ilike('%{0}%'.format(keyword))
                ).order_by(email.date).all()
        mails.extend(self.session.query(email).filter(
                email.subject.ilike('%{0}%'.format(keyword))
                ).order_by(email.date).all())
        mails.reverse()
        #return list(set(mails))
        return mails

    def search_content_subject_index(self, list_name, keyword, limit=None,
                                     offset=None):
        """ Returns a list of email containing the specified keyword in
        their content or their subject.

        :arg list_name, name of the mailing list in which this email
        should be searched.
        :arg keyword, keyword to search in the content or subject of
        the emails.
        """
        criterion = ("to_tsvector('english', (content || ' ') || subject) "
                     "@@ to_tsquery(:keyword)")
        keyword = '%s:*' % keyword
        email = get_class_object(list_to_table_name(list_name), 'email',
                                 self.metadata)
        q = self.session.query(email)
        q = q.filter(criterion).params(keyword=keyword)
        # q = q.order_by(email.date)
        if limit is not None:
            # imply that the result set is that big
            q = q.offset(offset).limit(limit)
        return q.all()

    def search_content_subject_index_or(self, list_name, keyword, limit=None,
                                        offset=None):
        """ Returns a list of email containing the specified keyword in
        their content or their subject.

        :arg list_name, name of the mailing list in which this email
        should be searched.
        :arg keyword, keyword to search in the content or subject of
        the emails.
        """
        criterion_subject = ("to_tsvector('english', subject) "
                             "@@ to_tsquery(:keyword)")
        criterion_content = ("to_tsvector('english', content) "
                             "@@ to_tsquery(:keyword)")
        keyword = '%s:*' % keyword
        email = get_class_object(list_to_table_name(list_name), 'email',
                                 self.metadata)
        q = self.session.query(email).filter(
            or_(criterion_subject,
                criterion_content)).params(keyword=keyword)
        if limit is not None:
            # imply that the result set is that big
            q = q.offset(offset).limit(limit)
        return q.all()

    def search_content_subject_cs(self, list_name, keyword):
        """ Returns a list of email containing the specified keyword in
        their content or their subject.

        :arg list_name, name of the mailing list in which this email
        should be searched.
        :arg keyword, keyword to search in the content or subject of
        the emails.
        """
        email = get_class_object(list_to_table_name(list_name), 'email',
            self.metadata)
        mails = self.session.query(email).filter(
                email.content.like('%{0}%'.format(keyword))
                ).order_by(email.date).all()
        mails.extend(self.session.query(email).filter(
                email.subject.like('%{0}%'.format(keyword))
                ).order_by(email.date).all())
        mails.reverse()
        #return list(set(mails))
        return mails

    def search_content_subject_or(self, list_name, keyword, limit=None,
                                  offset=None):
        """ Returns a list of email containing the specified keyword in
        their content or their subject.

        :arg list_name, name of the mailing list in which this email
        should be searched.
        :arg keyword, keyword to search in the content or subject of
        the emails.
        """
        email = get_class_object(list_to_table_name(list_name), 'email',
            self.metadata)
        mails = self.session.query(email).filter(or_(
                email.content.ilike('%{0}%'.format(keyword)),
                email.subject.ilike('%{0}%'.format(keyword))
                )).order_by(email.date)

        if limit is not None:
            # imply that the result set is that big
            mails = mails.offset(offset).limit(limit)

        mails = mails.all()
        mails.reverse()
        return list(set(mails))

    def search_content_subject_or_cs(self, list_name, keyword, limit=None,
                                     offset=None):
        """ Returns a list of email containing the specified keyword in
        their content or their subject.

        :arg list_name, name of the mailing list in which this email
        should be searched.
        :arg keyword, keyword to search in the content or subject of
        the emails.
        """
        email = get_class_object(list_to_table_name(list_name), 'email',
            self.metadata)
        mails = self.session.query(email).filter(or_(
                email.content.like('%{0}%'.format(keyword)),
                email.subject.like('%{0}%'.format(keyword))
                )).order_by(email.date)

        if limit is not None:
            # imply that the result set is that big
            mails = mails.offset(offset).limit(limit)

        mails = mails.all()

        mails.reverse()
        return list(set(mails))

    def search_sender(self, list_name, keyword):
        """ Returns a list of email containing the specified keyword in
        the name or email address of the sender of the email.

        :arg list_name, name of the mailing list in which this email
        should be searched.
        :arg keyword, keyword to search in the database.
        """
        email = get_class_object(list_to_table_name(list_name), 'email',
            self.metadata)
        mails = self.session.query(email).filter(
                email.sender.ilike('%{0}%'.format(keyword))
                ).order_by(email.date).all()
        mails.extend(self.session.query(email).filter(
                email.email.ilike('%{0}%'.format(keyword))
                ).order_by(email.date).all())
        mails.reverse()
        #return list(set(mails))
        return mails

    def search_sender_cs(self, list_name, keyword):
        """ Returns a list of email containing the specified keyword in
        the name or email address of the sender of the email.

        :arg list_name, name of the mailing list in which this email
        should be searched.
        :arg keyword, keyword to search in the database.
        """
        email = get_class_object(list_to_table_name(list_name), 'email',
            self.metadata)
        mails = self.session.query(email).filter(
                email.sender.like('%{0}%'.format(keyword))
                ).order_by(email.date).all()
        mails.extend(self.session.query(email).filter(
                email.email.like('%{0}%'.format(keyword))
                ).order_by(email.date).all())
        mails.reverse()
        #return list(set(mails))
        return mails

    def search_sender_or(self, list_name, keyword):
        """ Returns a list of email containing the specified keyword in
        the name or email address of the sender of the email.

        :arg list_name, name of the mailing list in which this email
        should be searched.
        :arg keyword, keyword to search in the database.
        """
        email = get_class_object(list_to_table_name(list_name), 'email',
            self.metadata)
        mails = self.session.query(email).filter(or_(
                email.sender.ilike('%{0}%'.format(keyword)),
                email.email.ilike('%{0}%'.format(keyword))
                )).order_by(email.date).all()
        mails.reverse()
        return list(set(mails))

    def search_sender_or_cs(self, list_name, keyword):
        """ Returns a list of email containing the specified keyword in
        the name or email address of the sender of the email.

        :arg list_name, name of the mailing list in which this email
        should be searched.
        :arg keyword, keyword to search in the database.
        """
        email = get_class_object(list_to_table_name(list_name), 'email',
            self.metadata)
        mails = self.session.query(email).filter(or_(
                email.sender.like('%{0}%'.format(keyword)),
                email.email.like('%{0}%'.format(keyword))
                )).order_by(email.date).all()
        mails.reverse()
        return list(set(mails))

    def search_subject(self, list_name, keyword):
        """ Returns a list of email containing the specified keyword in
        their subject.

        :arg list_name, name of the mailing list in which this email
        should be searched.
        :arg keyword, keyword to search in the subject of the emails.
        """
        email = get_class_object(list_to_table_name(list_name), 'email',
            self.metadata)
        mails = self.session.query(email).filter(
                email.subject.ilike('%{0}%'.format(keyword))
                ).order_by(email.date).all()
        mails.reverse()
        return mails

    def search_subject_cs(self, list_name, keyword):
        """ Returns a list of email containing the specified keyword in
        their subject.

        :arg list_name, name of the mailing list in which this email
        should be searched.
        :arg keyword, keyword to search in the subject of the emails.
        """
        email = get_class_object(list_to_table_name(list_name), 'email',
            self.metadata)
        mails = self.session.query(email).filter(
                email.subject.like('%{0}%'.format(keyword))
                ).order_by(email.date).all()
        mails.reverse()
        return mails
