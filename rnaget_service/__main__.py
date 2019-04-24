#!/usr/bin/env python3
"""
Driver program for service
"""
import sys
import argparse
import logging
import pkg_resources
import connexion
import os

from tornado.options import define
import rnaget_service.orm
import rnaget_service.orm.demo_data as demo


def main(args=None):
    """The main routine."""
    if args is None:
        args = sys.argv[1:]

    parser = argparse.ArgumentParser('Run RNA Get service')
    parser.add_argument('--host', required=True)
    parser.add_argument('--database', default='demo')
    parser.add_argument('--port', default=3000)
    parser.add_argument('--logfile', default="./log/rnaget.log")
    parser.add_argument('--loglevel', default='INFO',
                        choices=['DEBUG', 'INFO', 'WARN', 'ERROR', 'CRITICAL'])
    args = parser.parse_args(args)

    # set up the application
    app = connexion.FlaskApp(__name__, server='tornado')
    app.app.config['SERVER_NAME'] = str(args.host) + ':' + str(args.port)

    # set up the demo application
    if args.database == 'demo':
        print("*** Service started with DEMO DB")
        demo_db = demo.get_db(app)
        define("dbfile", default=demo_db)

        if os.path.exists(demo_db):
            os.remove(demo_db)

        rnaget_service.orm.init_db()
        demo.setup_db(app)
    else:
        define("dbfile", default=args.database)
        rnaget_service.orm.init_db()

    db_session = rnaget_service.orm.get_session()

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

    # add the swagger APIs
    api_def = pkg_resources.resource_filename('rnaget_service',
                                              'api/rnaget.yaml')
    app.add_api(api_def, strict_validation=True, validate_responses=True)

    print("Running on: http://{}".format(app.app.config['SERVER_NAME']))
    app.run(host=args.host, port=args.port)


if __name__ == "__main__":
    main()
