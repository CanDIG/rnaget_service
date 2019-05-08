#!/usr/bin/env python3
"""
Driver program for service
"""
import sys
import argparse
import logging
import pkg_resources
import connexion

from tornado.options import define
import candig_rnaget.orm

# Expose WSGI application
app = connexion.FlaskApp(__name__, server='tornado')
api_def = pkg_resources.resource_filename('candig_rnaget',
                                          'api/rnaget.yaml')
app.add_api(api_def, strict_validation=True, validate_responses=True)
application = app.app


def main(args=None):
    """The main routine."""
    if args is None:
        args = sys.argv[1:]

    parser = argparse.ArgumentParser('Run RNA Get service')
    parser.add_argument('--host', required=True)
    parser.add_argument('--database', default='./data/rnaget.db')
    parser.add_argument('--port', default=3000)
    parser.add_argument('--logfile', default="./log/rnaget.log")
    parser.add_argument('--loglevel', default='INFO',
                        choices=['DEBUG', 'INFO', 'WARN', 'ERROR', 'CRITICAL'])
    parser.add_argument('--tmpdata', default="./data/tmp/")
    args = parser.parse_args(args)

    # set up the local application
    app.app.config['BASE_DL_URL'] = 'http://'+str(args.host)+':'+str(args.port)
    app.app.config['TMP_DIRECTORY'] = args.tmpdata

    define("dbfile", default=args.database)
    candig_rnaget.orm.init_db()
    db_session = candig_rnaget.orm.get_session()

    @app.app.teardown_appcontext
    def shutdown_session(exception=None):  # pylint:disable=unused-variable,unused-argument
        """
        Tear down the DB session
        """
        db_session.remove()

    # configure logging
    log_handler = logging.FileHandler(args.logfile)
    numeric_loglevel = getattr(logging, args.loglevel.upper())
    log_handler.setLevel(numeric_loglevel)

    app.app.logger.addHandler(log_handler)
    app.app.logger.setLevel(numeric_loglevel)

    app.run(host=args.host, port=args.port)


if __name__ == "__main__":
    main()
