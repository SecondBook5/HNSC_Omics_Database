from db.mapping_table import MappingTable
from config.db_config import get_session_context
from config.logger_config import configure_logger
from kegg_api import fetch_kegg_pathways  # Assume an existing helper function

logger = configure_logger(name="KEGGEnrichment", log_file=snakemake.output.log, output="file")

def enrich_kegg_data():
    with get_session_context() as session:
        entries = session.query(MappingTable).filter(MappingTable.pathways.is_(None)).all()
        for entry in entries:
            kegg_gene_id = entry.ensembl_gene_id
            if not kegg_gene_id:
                continue
            pathways = fetch_kegg_pathways(kegg_gene_id)
            if pathways:
                entry.pathways = pathways
        session.commit()

if __name__ == "__main__":
    enrich_kegg_data()
