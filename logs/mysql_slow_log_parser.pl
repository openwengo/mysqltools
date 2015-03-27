#!/usr/bin/perl 
#
# Nathanial Hendler
# http://retards.org/
#
# 2001-06-26 v1.0
#
# This perl script parses a MySQL slow_queries log file
# ignoring all queries less than $min_time and prints
# out how many times a query was greater than $min_time
# with the seconds it took each time to run.  The queries
# are sorted by number of times it took; the most often
# query appearing at the bottom of the output.
#
# Usage: mysql_slow_log_parser logfile
#
# ------------------------
# SOMETHING TO THINK ABOUT (aka: how to read output)
# ------------------------
#
# Also, it does to regex substitutions to normalize
# the queries...
#
#   $query_string =~ s/\d+/XXX/g;
#   $query_string =~ s/([\'\"]).+?([\'\"])/$1XXX$2/g;
#
# These replace numbers with XXX and strings found in
# quotes with XXX so that the same select statement
# with different WHERE clauses will be considered
# as the same query.
#
# so these...
#
#   SELECT * FROM offices WHERE office_id = 3;
#   SELECT * FROM offices WHERE office_id = 19;
#
# become...
#
#   SELECT * FROM offices WHERE office_id = XXX;
#
#
# And these...
#
#   SELECT * FROM photos WHERE camera_model LIKE 'Nikon%';
#   SELECT * FROM photos WHERE camera_model LIKE '%Olympus';
#
# become...
#
#   SELECT * FROM photos WHERE camera_model LIKE 'XXX';
#
#
# ---------------------
# THIS MAY BE IMPORTANT (aka: Probably Not)
# --------------------- 
#
# *SO* if you use numbers in your table names, or column
# names, you might get some oddities, but I doubt it.
# I mean, how different should the following queries be
# considered?
#
#   SELECT car1 FROM autos_10;
#   SELECT car54 FROM autos_11;
#
# I don't think so.
#
# This script has been altered for handling extended informations in the logs
# Some of them are only available if the option 
# log_slow_verbosity = full
# is set. This option is available only on Percona Server


$min_time       = 0;	# Skip queries less than $min_time
$min_rows	= 0;
$max_display    = 10;	# Truncate display if more than $max_display occurances of a query

print "\n Starting... \n";

$query_string   = '';
$time           = 0;
$new_sql        = 0;


##############################################
# Loop Through The Logfile
##############################################

while (<>) {

	# Skip Bogus Lines

	next if ( m|/.*mysqld, Version:.+ started with:| );
	next if ( m|Tcp port: \d+  Unix socket: .*mysql.sock| );
	next if ( m|Time\s+Id\s+Command\s+Argument| );
	next if ( m|administrator\s+command:| );
	next if ( /^# User\@Host/ );
	next if ( /^# Schema/ );

# # Time: 150326 19:00:05
# # User@Host: devispresto[devispresto] @  [xx.xx.xx.xx]  Id: 905481
# # Schema: devispresto  Last_errno: 0  Killed: 0
# # Query_time: 1.697728  Lock_time: 0.000025  Rows_sent: 6  Rows_examined: 795599  Rows_affected: 0
# # Bytes_sent: 230  Tmp_tables: 1  Tmp_disk_tables: 0  Tmp_table_sizes: 126992
# # InnoDB_trx_id: xx
# # QC_Hit: No  Full_scan: No  Full_join: No  Tmp_table: Yes  Tmp_table_on_disk: No
# # Filesort: Yes  Filesort_on_disk: No  Merge_passes: 0
# #   InnoDB_IO_r_ops: 0  InnoDB_IO_r_bytes: 0  InnoDB_IO_r_wait: 0.000000
# #   InnoDB_rec_lock_wait: 0.000000  InnoDB_queue_wait: 0.000000
# #   InnoDB_pages_distinct: 16040
# #   InnoDB_trx_id: 2D06FCC
# QC_Hit: No  Full_scan: No  Full_join: No  Tmp_table: No  Tmp_table_on_disk: No
# SET timestamp=1427392805;
# SELECT /* NOTAG */ `m`.`user_group_id`, COUNT(m.member_id) AS `ct` FROM `members` AS `m` WHERE (m.user_id IS NULL) AND (m.FK_category_id = 1) AND (user_group_id IS NOT NULL) AND (member_status IN ('inactive', 'inactive temp')) GROUP BY `user_group_id`;


	# print $_;
	# if ( /Query_time:\s+(.*)\s+Lock_time:\s+(.*)\s/ ) {
	#if ( /Query_time:\s+(.*)\s+Lock_time:\s+(.*)\s+Rows_examined:\s+(\d+)/ ) {
	if ( /Query_time:\s+(.*)\s+Lock_time:\s+(.*)\s+Rows_examined:\s+(.*)/ ) {

		$time    = $1;
		$rows	 = $3;
		$new_sql = 1;
		$bytes_sent    = 0;
		$tmp_tables    = 0;
		$tmp_disk_tables    = 0;
		$tmp_table_sizes    = 0;
		$file_sort = 0;
		$file_sort_on_disk = 0;
		$qc_hit = 0;
		$full_scan = 0;
		$full_join = 0;
		$merge_passes = 0;
		# print "found $1 $3\n";
		next;

	}

	# Percona extensions
	if ( /Bytes_sent:\s+(.*)\s+Tmp_tables:\s+(.*)\s+Tmp_disk_tables:\s+(.*)Tmp_table_sizes:\s+(.*)/ ) {

		$bytes_sent    = $1;
		$tmp_tables    = $2;
		$tmp_disk_tables    = $3;
		$tmp_table_sizes    = $4;
		# print "found $1 $3\n";
		next;

	}

	if ( /Filesort:\s+(.*)\s+Filesort_on_disk:\s+(.*)\s+Merge_passes:\s+(.*)/ ) {

		($a , $b , $c ) = ( $1, $2, $3);
		$file_sort = 1 if $a =~ /Yes/ ;
		$file_sort_on_disk = 1 if $b =~ /Yes/ ;
		$merge_passes = $c;
		#print $file_sort,$file_sort_on_disk,$merge_passes,"\n";
		next;

	}

        # QC_Hit: No  Full_scan: No  Full_join: No  Tmp_table: No  Tmp_table_on_disk: No
	if ( /QC_Hit:\s+(.*)\s+Full_scan:\s+(.*)\s+Full_join:\s+(.*)\s+Tmp_table:/ ) {
		($a , $b , $c, $d ) = ( $1, $2, $3, $4);
		$qc_hit = 1 if $a =~ /Yes/ ;
		$full_scan = 1 if $b =~ /Yes/ ;
		$full_join = 1 if $c =~ /Yes/ ;
		#print $qc_hit,$full_scan,$full_join,"\n" ;
		next;
	}

# #   InnoDB_IO_r_ops: 0  InnoDB_IO_r_bytes: 0  InnoDB_IO_r_wait: 0.000000
# #   InnoDB_rec_lock_wait: 0.000000  InnoDB_queue_wait: 0.000000
# #   InnoDB_pages_distinct: 16040
# #   InnoDB_trx_id: 2D00CF4

	if ( /InnoDB_IO/ ) {
		next ;
	}
	if ( /InnoDB_rec_lock/ ) {
		next ;
	}
	if ( /InnoDB_pages_distinct/ ) {
		next ;
	}
	if ( /InnoDB_trx_id/ ) {
		next ;
	}

	if ( /^\#/ && $query_string ) {

			if (($time > $min_time) && ($rows >= $min_rows)) {
				$orig_query = $query_string;

				$query_string =~ s,/\*[^\*]+\*/,/* comment */,g;
				$query_string =~ s/\d+/XXX/g;
				$query_string =~ s/'([^'\\]*(\\.[^'\\]*)*)'/'XXX'/g;
				$query_string =~ s/"([^"\\]*(\\.[^"\\]*)*)"/"XXX"/g;
				#$query_string =~ s/([\'\"]).+?([\'\"])/$1XXX$2/g;
				#$query_string =~ s/\s+/ /g;
				#$query_string =~ s/\n+/\n/g;

				push @{$queries{$query_string}}, $time;
				push @{$queries_rows{$query_string}}, $rows;
				$queries_tot{$query_string} += $time;
				$queries_orig{$query_string} = $orig_query;
				$queries_tmp_tables{$query_string} += $tmp_tables;
				$queries_tmp_disk_tables{$query_string} += $tmp_disk_tables;
				$queries_tmp_table_sizes{$query_string} += $tmp_table_sizes;
				$queries_file_sort{$query_string} += $file_sort;
				$queries_file_sort_on_disk{$query_string} += $file_sort_on_disk;
				$queries_merge_passes{$query_string} += $merge_passes;
				$queries_qc_hit{$query_string} += $qc_hit;
				$queries_full_scan{$query_string} += $full_scan;
				$queries_full_join{$query_string} += $full_join;
				$query_string = '';

			}

	} else {
		
		if ($new_sql) {
			$query_string = $_;
			$new_sql = 0;
		} else {
			$query_string .= $_;
		}
	}

}


##############################################
# Display Output
##############################################

foreach my $query ( sort { $queries_tot{$b} <=> $queries_tot{$a} } keys %queries_tot )  {
	my $total = 0; 
	my $cnt = 0;
	my @seconds = sort { $a <=> $b } @{$queries{$query}};
	my @rows    = sort { $a <=> $b } @{$queries_rows{$query}};
	($total+=$_) for @seconds;
	($cnt++) for @seconds;

	print "### " . @{$queries{$query}} . " Quer" . ((@{$queries{$query}} > 1)?"ies ":"y ") . "\n";
	print "### Total time: " . $total .", Average time: ".($total/$cnt)."\n";
	print "### Taking ";
	print @seconds > $max_display ? "$seconds[0] to $seconds[-1]" : sec_joiner(\@seconds);
	print " seconds to complete\n";
	print "### Rows analyzed ";
        print @rows > $max_display ? "$rows[0] - $rows[-1]": sec_joiner(\@rows);
	print "\n";
	print "### extra: tmp_tables:",  $queries_tmp_tables{$query}, " tmp_disk_tables:", $queries_tmp_disk_tables{$query}, " tmp_table_sizes:", $queries_tmp_table_sizes{$query}  /  @{$queries{$query}} , "\n";
	print "### extra: file_sort:",  $queries_file_sort{$query}, " file_sort_on_disk:", $queries_file_sort_on_disk{$query}, " merge_passes:", $queries_merge_passes{$query}  /  @{$queries{$query}} , "\n";
	print "### extra: qc_hits:",  $queries_qc_hit{$query}, " full_join:", $queries_full_join{$query}, " full_scan:", $queries_full_scan{$query} , "\n";
	
	print "$query\n";
	print $queries_orig{$query}."\n\n";
}


sub sec_joiner {
	my ($seconds) = @_;
	$string = join(", ", @{$seconds});
	$string =~ s/, (\d+)$/ and $1/;
	return $string;
}

exit(0);
