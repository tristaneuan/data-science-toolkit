from . import get_db_and_cursor
from argparse import ArgumentParser, Namespace
from multiprocessing import Pool
import requests


def get_args():
    ap = ArgumentParser()
    ap.add_argument(u'--host', dest=u'host', default=u'localhost')
    ap.add_argument(u'-u', u'--user', dest=u'user', default=u'root')
    ap.add_argument(u'-p', u'--password', dest=u'password', default=u'root')
    ap.add_argument(u'-d', u'--database', dest=u'database', default=u'authority')
    ap.add_argument(u'-n', u'--num-processes', dest=u'num_processes', type=int, default=6)
    return ap.parse_known_args()


def get_pageviews_for_wiki(args):
    db, cursor = get_db_and_cursor(args)
    _, wiki_id, url = args.row
    cursor.execute(u"SELECT article_id FROM articles WHERE wiki_id = %d" % wiki_id)
    params = {
        u'controller': u'WikiaSearchIndexerController',
        u'method': u'get',
        u'service': u'Metadata'
    }
    print url, cursor.rowcount, u"rows"
    while True:
        rows = cursor.fetchmany(15)
        if not rows:
            break
        params[u'ids'] = u'|'.join([apply(str, x) for x in rows])
        try:
            response = requests.get(u"%swikia.php" % url, params=params).json()
        except ValueError:
            continue
        updates = [(doc[u'id'], doc.get(u"views", {}).get(u"set", 0))
                   for doc in response.get(u"contents", {})]
        cases = u"\n".join([u"WHEN \"%s\" THEN %d" % update for update in updates])
        cursor.execute(u"""
            UPDATE articles
            SET pageviews = CASE
            %s
            END
            WHERE doc_id IN ("%s")""" % (cases, u"\",\"".join(map(lambda y: y[0], updates))))
        db.commit()
    print u"done with", url


def main():
    args, _ = get_args()
    db, cursor = get_db_and_cursor(args)
    p = Pool(processes=args.num_processes)
    cursor.execute(u"SELECT wiki_id, url FROM wikis ")
    for i in range(0, cursor.rowcount, 500):
        print i
        p.map_async(get_pageviews_for_wiki, [Namespace(row=row, **vars(args)) for row in cursor.fetchmany(500)]).get()




if __name__ == u'__main__':
    main()