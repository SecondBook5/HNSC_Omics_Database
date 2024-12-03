# Snakefile: GEO Metadata Pipeline

# Load the configuration from the YAML file in the root directory
configfile: "config.yaml"

# Define the output directory for the GEO metadata
output_dir = config["output_directories"]["geo_metadata"]["raw"]

# Rule for downloading GEO metadata files
rule download_geo_metadata:
    """
    Downloads GEO metadata files as specified in the configuration.
    """
    input:
        lambda wildcards: config["geo_metadata"]["geo_ids"]
    output:
        directory(output_dir)
    shell:
        """
        python pipeline/geo_pipeline/geo_metadata_downloader.py \
        --geo_ids {input} \
        --output_dir {output}
        """
