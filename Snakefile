configfile: "config.yaml"

import pandas as pd

rule download_GWAS_results:
	output:
		"{dir}sumstats_{id, [0-9]+}.tsv.gz"
	shell:
		f"wget -q -O {{output}} 'https://pan-ukb-us-east-1.s3.amazonaws.com/sumstats_flat_files/biomarkers-{{wildcards.id}}-both_sexes-irnt.tsv.bgz'"

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
        
rule preprocess_p:
	input:
		"{dir}sumstats_{id, [0-9]+}.tsv"
	output:
		"{dir}p_{id, [0-9]+}.tsv"
	shell:
		f"python preprocess.py {{input}} {{output}} {config['threshold']}"
        
rule get_var_list:
	input:
		"{dir}p_{id, [0-9]+}.tsv"
	output:
		"{dir}varlist_{id, [0-9]+}.tsv"
	shell:   
		r"awk '{{print $8}}' {input} > {output}"
        
rule create_subset_binary:
	input:
		"{dir}varlist_{id, [0-9]+}.tsv"
	output:
		"{dir}subset_binary_{id, [0-9]+}.bed"
	shell:
		f"plink2 --pfile {config['plink2_binary_files']} --extract {{input}} --make-bed --out {{wildcards.dir}}subset_binary_{{wildcards.id}}"
        
rule clump:
	input:
		gwas_res="{dir}p_{id, [0-9]+}.tsv",
		subset_binary="{dir}subset_binary_{id, [0-9]+}.bed"
	output:
		"{dir}GWAS_variants_{id, [0-9]+}.clumped.vars"
	shell:
		f"plink -bfile {{wildcards.dir}}subset_binary_{{wildcards.id}} --clump {{input.gwas_res}} --clump-snp-field varid --clump-field {config['p_val_col']} --clump-p1 {config['threshold']} --clump-p2 {config['threshold']} --out {{wildcards.dir}}GWAS_variants_{{wildcards.id}}" + "\n" + r"awk '{{print $3}}' {wildcards.dir}GWAS_variants_{wildcards.id}.clumped > {output}"
        
rule get_mac:
	input:
		"{dir}GWAS_variants_{id, [0-9]+}.clumped.vars"
	output:
		"{dir}GWAS_variants_clumped_mac_{id, [0-9]+}.raw"
	shell:
		f"plink2 --pfile {config['plink2_binary_files']} --extract {{input}} --export A --out {{wildcards.dir}}GWAS_variants_clumped_mac_{{wildcards.id}}"
        
rule write_parquet:
	input:
		"{dir}GWAS_variants_clumped_mac_{id, [0-9]+}.raw"
	output:
		"{dir}GWAS_variants_clumped_mac_{id, [0-9]+}.parquet"
	shell:
		f"python create_parquet.py {{input}} {{output}}"
