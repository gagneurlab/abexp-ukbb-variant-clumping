configfile: "config.yaml"

import pandas as pd

rule download_GWAS_results:
	output:
		"{dir}sumstats_{id, [0-9]+}.tsv.gz"
	shell:
		f"wget -O {{output}} 'https://pan-ukb-us-east-1.s3.amazonaws.com/sumstats_flat_files/biomarkers-{{wildcards.id}}-both_sexes-irnt.tsv.bgz'"

rule extract_GWAS_results:
	input:
		"{dir}sumstats_{id, [0-9]+}.tsv.gz"
	output:
		"{dir}sumstats_{id, [0-9]+}.tsv"
	shell:
		r"""
		gzip -d -k {input}
		cut -f 1,2,3,4,13 {output} > {output}.temp
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
        
rule clump:
	input:
		"{dir}p_{id, [0-9]+}.tsv"
	output:
		"{dir}WES_200K_{id, [0-9]+}.clumped"
	shell:
		f"plink -bfile {config['plink_binary_files']} --clump {{input}} --clump-snp-field varid --clump-field pval_meta_raw --clump-p1 {config['threshold']} --clump-p2 {config['threshold']} --out {{wildcards.dir}}WES_200K_{{wildcards.id}}"
        
rule extract_index_vars:
	input:
		"{dir}WES_200K_{id, [0-9]+}.clumped"
	output:
		"{dir}WES_200K_index_vars_{id, [0-9]+}.ped"
	shell:
		r"awk '{{print $3}}' {input} > {input}.vars" + "\n" + f"plink --bfile {config['plink_binary_files']} --extract {{input}}.vars --recode --out {{wildcards.dir}}WES_200K_index_vars_{{wildcards.id}}"
        
rule get_mac:
	input:
		"{dir}WES_200K_index_vars_{id, [0-9]+}.ped"
	output:
		"{dir}WES_200K_index_vars_mac_{id, [0-9]+}.parquet"
	shell:
		"python mac.py {input} {output}"