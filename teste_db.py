# Note: the module name is psycopg, not psycopg3
import psycopg


def get_produtos():
    try:
        sql_produtos_alterdata = """
select p.idproduto,d.cdprincipal,c.dscodigo ,d.dsdetalhe ,d.vlprecovenda 
from produto p 
join detalhe d on (p.idproduto=d.idproduto) 
left outer JOIN codigos c on (c.idproduto =p.idproduto and tpcodigo='EAN13')
where d.cdprincipal in ('003523', '010963', '017504', '017505') 
group by p.idproduto,d.cdprincipal,c.dscodigo ,d.dsdetalhe ,d.vlprecovenda limit 50"""
        host = 'localhost'
        user = 'postgres'
        passw = '#abc123#'
        db = 'ALTERDATA_SHOP'
        schema = 'wshop'

        # Connect to an existing database
        print(f'connect DB: {host}:{db}')
        # "postgresql://user:user_password@db_server_hostname:5432
        with psycopg.connect(f"postgresql://{user}:{passw}@{host}:5432/{db}?options=-csearch_path%3D{schema},public") as conn:
            with conn.cursor() as cur:
              # Query the database and obtain data as Python objects.
                cur.execute(sql_produtos_alterdata)
                cur.fetchone()

                for record in cur:
                    print(record)
    except psycopg.Error as err:
        print(f"Oops! An exception has occured: {err}")
        print(f"Exception TYPE: {type(err)}")


if __name__ == '__main__':
    get_produtos()

