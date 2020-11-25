from flask_restful import Resource
import publisher_discovery
 


class PublisherDiscovery(Resource):
    def get(self):
        unique_countries = publisher_discovery.unique_countries
        country_publisher_dict = publisher_discovery.country_publisher_dict

        result = publisher_discovery.process_all_functions(unique_countries, country_publisher_dict )

        return result

