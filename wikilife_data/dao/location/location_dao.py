# coding=utf-8

from wikilife_data.dao.base_dao import BaseDAO


class LocationDAO(BaseDAO):
    """
    Model:
    {
       "geo": {
           "lat": -34.577089,
           "lon": -58.40139,
           "alt": 10
       },
       "political": {
           "continent": "America",
           "country": "Argentina",
           "state": "Buenos Aires",
           "city": "Ciudad autónoma de Buenos Aires",
           "area": "Palermo"
       }
   }
    
    Schema:
      geonameid integer NOT NULL,
      name character varying(200),
      asciiname character varying(200),
      alternatenames character varying(5000),
      latitude double precision,
      longitude double precision,
      fclass character(1),
      fcode character varying(10),
      country character(2),
      cc2 character varying(60),
      admin1 character varying(20),
      admin2 character varying(80),
      admin3 character varying(20),
      admin4 character varying(20),
      population bigint,
      elevation integer,
      dem integer,
      timezone character varying(40),
      modification_date text,
      CONSTRAINT firstkey PRIMARY KEY (geonameid)
    """

    
    def _initialize(self):
        pass

    def search_location(self, name, country=None, feature_class=None, feature_code=None, limit=10):
        """
        country: ISO 3166-2
        """
        statement = "SELECT * FROM geoname where name ILIKE %(name)s"
        name = "%%%s%%" %name
        query_args = {"name": name}

        if country:
            statement += " and country=%(country)s"
            query_args["country"] = country

        if feature_class:
            statement += " and fclass=%(feature_class)s"
            query_args["feature_class"] = feature_class

        if feature_code:
            statement += " and fcode=%(feature_code)s"
            query_args["feature_code"] = feature_code

        statement += " LIMIT %s;" %limit

        conn = self.get_db()
        cursor = conn.cursor()
        print cursor.mogrify(statement, query_args)
        cursor.execute(statement, query_args)
        results = cursor.fetchall()

        locations = []
        for result in results:
            location = self._to_model(result)
            locations.append(location)

        cursor.close()
        #conn.close()

        return locations

    def _to_model(self, location):
        #TODO
        return {
           "id": location[0],
           "name": location[2],
           "feature_class": location[6],
           "feature_code": location[7],
           "geo": {
               "lat": location[4],
               "lon": location[5],
               "alt": location[15]
           },
           "political": {
               "continent": location[10],
               "country": location[8],
               "state": location[11],
               "city": location[12],
               "area": location[13],
               "tz": location[17]
           }
       }
        
