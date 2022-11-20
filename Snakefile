configfile: "config.yaml"

#Downloads GWAS-results from the UK-Biobank for a given phenocode. The trait_type specified in the config should match the trait_type of the phenocode.
rule download_GWAS_results:
	output:
		"{dir}sumstats_{id, [0-9]+}.tsv.gz"
	shell:
		f"wget -q -O {{output}} 'https://pan-ukb-us-east-1.s3.amazonaws.com/sumstats_flat_files/{config['trait_type']}-{{wildcards.id}}-both_sexes-irnt.tsv.bgz'"

#Unzips the GWAS-results and removes unnecessary columns
rule extract_GWAS_results:
	input:
		"{dir}sumstats_{id, [0-9]+}.tsv.gz"
	output:
		"{dir}sumstats_{id, [0-9]+}.tsv"
	shell:
		r"""
		gzip -d -k {input}
		cut -f 1,2,3,4,8,13 {output} > {output}.temp
		rm {output}
		mv {output}.temp {output} 
		"""

#Uses a python script in order to convert the GWAS log p values into normal values. Also sets the variant IDs to match with the plink2 binary genotype data. Only variants with a p-value below the threshold are written into the output.
rule preprocess_p:
	input:
		"{dir}sumstats_{id, [0-9]+}.tsv"
	output:
		"{dir}p_{id, [0-9]+}.tsv"
	shell:
		f"python preprocess.py {{input}} {{output}} {config['p_threshold']}"

#Extracts the column containing the variant IDs. This format can then be used by plink2 as a filter.
rule get_var_list:
	input:
		"{dir}p_{id, [0-9]+}.tsv"
	output:
		"{dir}varlist_{id, [0-9]+}.tsv"
	shell:   
		r"awk '{{print $8}}' {input} > {output}"

#Using plink2 the genotype data is extracted for the significant variants and written into plink1.9 format. This is necessary because plink2 does not support clumping (yet).
rule create_subset_binary:
	input:
		"{dir}varlist_{id, [0-9]+}.tsv"
	output:
		"{dir}subset_binary_{id, [0-9]+}.bed"
	shell:
		f"plink2 --pfile {config['plink2_binary_files']} --extract {{input}} --make-bed --out {{wildcards.dir}}subset_binary_{{wildcards.id}}"

#Using plink1.9 the variants are clumped by LD and distance. The index variant IDs are written into the output file.
rule clump:
	input:
		gwas_res="{dir}p_{id, [0-9]+}.tsv",
		subset_binary="{dir}subset_binary_{id, [0-9]+}.bed"
	output:
		"{dir}GWAS_variants_{id, [0-9]+}.clumped.vars"
	shell:
		f"plink -bfile {{wildcards.dir}}subset_binary_{{wildcards.id}} --clump {{input.gwas_res}} --clump-snp-field varid --clump-field {config['p_val_col']} --clump-p1 {config['p_threshold']} --clump-p2 {config['p_threshold']} --clump-r2 {config['ld_threshold']} --clump-kb {config['kb_radius']} --out {{wildcards.dir}}GWAS_variants_{{wildcards.id}}" + "\n" + r"awk '{{print $3}}' {wildcards.dir}GWAS_variants_{wildcards.id}.clumped > {output}"

#Using plink2 the allele counts for all individuals are extracted from the genotype data for all index variants
rule get_mac:
	input:
		"{dir}GWAS_variants_{id, [0-9]+}.clumped.vars"
	output:
		"{dir}GWAS_variants_clumped_mac_{id, [0-9]+}.raw"
	shell:
		f"plink2 --pfile {config['plink2_binary_files']} --extract {{input}} --export A --out {{wildcards.dir}}GWAS_variants_clumped_mac_{{wildcards.id}}"

#For efficient columnar reading of allele counts by variant ID, the raw allele counts are written to a parquet file using a python script:
rule write_parquet:
	input:
		"{dir}GWAS_variants_clumped_mac_{id, [0-9]+}.raw"
	output:
		"{dir}GWAS_variants_clumped_mac_{id, [0-9]+}.parquet"
	shell:
		f"python create_parquet.py {{input}} {{output}}"
