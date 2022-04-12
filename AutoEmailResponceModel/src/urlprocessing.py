from urllib.parse import urlparse, unquote

class ParseURL(object):
    
    def __init__(self):
        pass
        
    @staticmethod
    def parse_url(url: str):
        if url is None:
            return None

        u_parse = None
        try:
            u_parse = urlparse(url)
        except:
            print('ERROR:', url)
            u_parse = None
        return u_parse

    @staticmethod
    def url_unquote(url:str):
        if url is None:
            return None

        utm_ref = unquote(url)
        if '%' in utm_ref:
            utm_ref = unquote(utm_ref)

        return utm_ref

    @staticmethod
    def get_utm_ref(url: str):
        if url is None:
            return None

        u_parse = ParseURL.parse_url(url)
        if u_parse is None:
            return None

        utm_ref = None
        if 'utm_referrer=' in u_parse.query:
            utm_ref = u_parse.query.split("utm_referrer=")[-1].split('&utm_')[0]
            if utm_ref == "":
                utm_ref = None

        if (utm_ref is not None) and ('http' not in utm_ref):
            utm_ref = "http://" + utm_ref

        return utm_ref

    @staticmethod
    def get_utm_campaign(url: str):
        if url is None:
            return None

        u_parse = ParseURL.parse_url(url)
        if u_parse is None:
            return None

        utm_camp = None
        if 'utm_campaign=' in u_parse.query:
            utm_camp = u_parse.query.split("utm_campaign=")[-1].split('&utm_')[0]
            if utm_camp == "":
                utm_camp = None

        return utm_camp    
    
    @staticmethod
    def get_sascamp(url: str):
        if url is None:
            return None

        u_parse = ParseURL.parse_url(url)
        if u_parse is None:
            return None

        utm_sas= None
        if 'sascamp=' in u_parse.query:
            utm_sas = 'CAMP'+u_parse.query.split("sascamp=")[-1].split('&sascamp')[0]
            if utm_sas == "":
                utm_sas = None

        return utm_sas  
    
    
    # TODO: utm_referrer=android-app://com.google.android.googlequicksearchbox -> android-app:
    @staticmethod
    def get_url_domain(url: str):
        if url is None:
            return None

        url_parse = ParseURL.parse_url(url)
        if url_parse is None:
            return None    

        url_domain = None
        if url_parse.netloc != "":
            url_domain = url_parse.netloc
        else:
            url_domain = "http://" + url_parse.path.lstrip("/")
            try:
                url_domain = urlparse(url_domain).netloc
            except:
                print('ERROR:', url)
                return None

        result = None
        if url_domain != "":
            result = url_domain
        return result

    @staticmethod
    def get_url_path(url: str):
        if url is None:
            return None

        url_parse = ParseURL.parse_url(url)
        if url_parse is None:
            return None

        url_path = None
        if url_parse.netloc != "":
            url_path = url_parse.path.strip("/")
        else:
            url = "http://" + url_parse.path.strip("/")
            try:
                url_path = urlparse(url).path.strip("/")
            except:
                print('ERROR', url)
                return None

        result = None
        if url_path != "":
            result = url_path

        return result

    @staticmethod
    def get_utm_term(url: str) -> str:
        if url is None:
            return None

        u_parse = ParseURL.parse_url(url)
        if u_parse is None:
            return None

        utm_term = None
        if 'utm_term=' in u_parse.query:
            parts = u_parse.query.split('&utm_term=')
            if len(parts) > 1:
                utm_term = parts[1].split("&")[0].replace("+", " ").strip()
            if utm_term == "":
                utm_term = None

        return utm_term

    @staticmethod
    def get_utm_query(url: str) -> str:
        if url is None:
            return None

        u_parse = ParseURL.parse_url(url)
        if u_parse is None:
            return None

        query = None    
        if 'q=' in u_parse.query:
            query_parts = u_parse.query.split('q=')
            if len(query_parts) > 1:
                query = query_parts[1].split("&")[0].replace("+", " ")
            if query == "":
                query = None

        return query

    @staticmethod
    def get_utm_source(url: str) -> str:
        if url is None:
            return None

        u_parse = ParseURL.parse_url(url)
        if u_parse is None:
            return None

        source = None

        if 'utm_source=' in u_parse.query:
            source_parts = u_parse.query.split('utm_source=')
            if len(source_parts) > 1:
                source = source_parts[1].split("&")[0]
            if source == "":
                source = None

        return source

    @staticmethod
    def get_utm_campaign(url: str) -> str:
        if url is None:
            return None

        u_parse = ParseURL.parse_url(url)
        if u_parse is None:
            return None

        campaign = None

        if 'utm_campaign=' in u_parse.query:
            campaign_parts = u_parse.query.split('utm_campaign=')
            if len(campaign_parts) > 1:
                campaign = campaign_parts[1].split("&")[0]
            if campaign == "":
                campaign = None

        return campaign
        
    @staticmethod
    def get_utm_medium(url: str) -> str:
        if url is None:
            return None

        u_parse = ParseURL.parse_url(url)
        if u_parse is None:
            return None

        medium = None

        if 'utm_medium=' in u_parse.query:
            medium_parts = u_parse.query.split('utm_medium=')
            if len(medium_parts) > 1:
                medium = medium_parts[1].split("&")[0]
            if medium == "":
                medium = None

        return medium