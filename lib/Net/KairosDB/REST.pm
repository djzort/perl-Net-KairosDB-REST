#!perl
# vim: softtabstop=4 tabstop=4 shiftwidth=4 ft=perl expandtab smarttab
# ABSTRACT: Perl API for KairosDB

package Net::KairosDB::REST;

use v5.10;
use Moo;
# VERSION
with 'WebService::Client';

use Time::HiRes qw/ gettimeofday /;
use URI::Encode::XS qw/ uri_encode /;
use Net::KairosDB::REST::Feature;
use Net::KairosDB::REST::Error;
use JSON::MaybeXS qw( JSON );
use List::Util::MaybeXS qw( uniq );

use namespace::clean;

has '+base_url' => ( default => 'http://localhost:8080' );
#has '+mode' => ( default => 'v2' ); # WebService::Client should return response objects
has 'api_version' => ( is => 'ro', default => 'v1' );

sub BUILD {
    my $self = shift;
    $self->ua->default_header(
        'User_Agent'    => sprintf(
            '%s %s (perl %s; %s)',
            __PACKAGE__,
            (__PACKAGE__->VERSION || 999999),
            $^V, $^O),
    );
}

{ # Wrap responses and convert to our errors

my $ss = sub {
    my $orig = shift;
    my $self = shift;
    my $result;
    eval { $result = $orig->($self,@_) };
    if ($@) {
        warn 'Error ref: ' . ref $@;
        warn 'Error: ' .$@ . 'END';
        Net::KairosDB::REST::Error->throw({
                code    => $@->code,
                content => $@->content,
                message => $@->message
            })
    }
    return $result
}; # my $ss

around 'get' => $ss;
around 'post' => $ss;
around 'delete' => $ss;

}

sub _mkuri {
    my $self = shift;
    my @paths = map { uri_encode( $_ ) } @_;
    return join '/',
        $self->base_url,
        'api',
        $self->api_version,
        @paths
}

sub _now_milliseconds {
    my ($seconds, $micro) = gettimeofday;
    my $milli = int($micro/1_000);
    return ($seconds * 1_000) +  $milli
}

sub _epoch_from_obj {
    my $ts = shift;

    if (ref $ts =~ m/^(?:DateTime|Time::(?:Moment|Piece))$/) {
        return $ts->epoch * 1_000
    }
    if (ref $ts eq 'Date::Manip::Date') {
        return $ts->printf('%s000');
    }
    die sprintf( 'Don\'t know how to serialize "timestamp" of ref %s', ref $ts )
}

# TODO datapoints_helper

=head2 datapoints

 my @datapoints;

 # Single value
 push @datapoints, {
      name      => "archive_file_search",
      timestamp => 1359786400000,
      value     => 321,
      tags => {
          host => "server2"
      }
 };

 # Multiple data points, single dimension
 push @datapoints, {
      name       => "archive_file_tracked",
      datapoints => [
                      [1359788400000, 123],
                      [1359788300000, 13.2],
                      [1359788410000, 23.1]
                    ],
      tags => {
          host        => "server1",
          data_center => "DC1"
      },
      ttl => 300
 };

 # Multiple data points, multi-dimension
 push @datapoints, {
      name => "impedance",
      type => "complex-number",
      datapoints => [
          [
              1359788400000,
              {
                  real      => 2.3,
                  imaginary => 3.4
              }
          ],
          [
              1359788300000,
              {
                  real      => 1.1,
                  imaginary => 5
              }
          ]
      ],
      tags => {
          host        => "server1",
          data_center => "DC1"
      }
 };

 my $result = $kdb->datapoints(\@datapoints);
 my $result = $kdb->datapoints(@datapoints);

Records metric data points.

See L<https://kairosdb.github.io/docs/build/html/restapi/AddDataPoints.html>

B<Parameters>

An array (or array ref) of datapoints, each is a hash reference.

There are three formats for datapoints as shown above, which can be mixed and matched.

This hash ref contains the following:

=over 4

=item name

Metric names must be unique. Multiple words in a metric name are typically
separated using an underscore ("_") to separate words such as archive_search.

This option is required.

=item timestamp

The timestamp is the date and time when the data was measured. It's a numeric
value that is the number of milliseconds since January 1st, 1970 UTC.

This option is required if a single I<value> option is provided.

=item value

The value is a number (i.e, 523 or 132.45).

You must provide either this option or I<datapoints>.

=item datapoints

An array reference of data points. Each data point consists of a I<timestamp> and I<value>.

You must provide either this option or a single I<value>.

=item tags

The tags field is a list of named properties. The tags are used when querying metrics to
narrow down the search.

For example, if multiple metrics are measured on server1, you could add the "host" => "server1"
tag to each of the metrics and queries could return all metrics for the "host" tagged with
the value of "server1".

At least one tag is required.

=item type

Type identifies custom data types. This field is only needed if the data value is something
other than a number. The type field is the name of the registered type for the custom data.

See L<Custom Types|https://kairosdb.github.io/docs/build/html/kairosdevelopment/CustomData.html> for information on custom types.

=item ttl

Sets the Cassandra ttl for the data points. In the example above the data points for metric
I<archive_file_tracked> will have the ttl set for 5 min.

Ommitting the ttl or setting it to 0 will use the default TTL value specified in settings.

=back

B<Returns>

Merely returns true (1) if successful.

B<Exceptions>

See also L<Net::KairosDB::REST::Error>

=over 4

=item 400 - Request Invalid

=item 500 - Internal Server Error

=back

=cut

sub datapoints {

    my $self = shift;

    my $datapoints;
    if (scalar @_ == 1) {
        $datapoints = shift;
        $datapoints = [ $datapoints ]
            if ref $datapoints eq 'HASH';
        die 'The "datapoints" value must be a hashref\'s or an arrayref'
            unless ref $datapoints eq 'ARRAY';
    }
    elsif (scalar @_ > 1) {
        $datapoints = [ @_ ]
    }
    else {
        die 'No "datapoints" provided'
    }

    for my $args (@$datapoints) {
        die 'All datapoints must be a hashref'
            if ref $args ne 'HASH';

        # name
        die 'All datapoints must have a "name"'
            unless $args->{name};
        die 'The datapoints "name" option must be a string'
            if ref $args->{name};

        # type
        if ($args->{type}) {
            die 'The datapoints "type" option must be a string'
                if ref $args->{type};
        }

        # ttl
        if ($args->{ttl}) {
            die 'The datapoints "ttl" option must be a string'
                if ref $args->{ttl};
            die 'The datapoints "ttl" option must be in seconds'
                if $args->{ttl} =~ m/[^0-9]/;
        }

        # value
        if ($args->{value}) {
            die 'Each datapoint may have a "value" or a list of "datapoints", not both'
                if $args->{datapoints};
            my $ts = $args->{timestamp};
            $ts = _epoch_from_obj( $ts )
                if ref $ts;
            if ($ts) {
                die 'The datapoints "timestamp" option must be milliseconds since epoch'
                    if $ts =~ m/[^0-9]/;
            }
            else {
                $ts = _now_milliseconds()
            }
            $args->{timestamp} = $ts;
        }

        # OR datapoints
        if ($args->{datapoints}) {
            die 'The "timestamp" option is only valid with a single "value", not a list of "datapoints"'
                if $args->{timestamp};
            die 'The "datapoints" option must be an array ref'
                if $args->{datapoints} ne 'ARRAY';
            for my $dp (@{$args->{datapoints}}) {
                die 'All values in "datapoints" array must be array refs'
                    if $dp ne 'ARRAY';
                die 'Two values (timestamp, value) must be provided for each item in the "datapoints" array'
                    if scalar @$dp != 2;
                $dp->[0] = _epoch_from_obj( $dp->[0] )
                    if ref $dp->[0];
            }
        }

        # tags
        die 'The "tags" option is required'
            unless $args->{tags};
        die 'The "tags" option must be a hash ref'
            if $args->{tags} ne 'HASH';
        die 'The "tags" option must have at lease one value'
            if scalar keys %{$args->{tags}} > 0;

        for my $v ( values %{$args->{tags}} ) {
            die 'The "tags" values must all be strings'
                if ref $v;
        }

    } # for my $args (@$datapoints)

    return $self->post(
        $self->_mkuri('datapoints'),
        $datapoints
    )

}

# delete datapoints
=head2 delete_datapoints

 my @datapoints;

 # Single value
 push @datapoints, {
      name      => "archive_file_search",
      timestamp => 1359786400000,
      value     => 321,
      tags => {
          host => "server2"
      }
 };

 # Multiple data points, single dimension
 push @datapoints, {
      name       => "archive_file_tracked",
      datapoints => [
                      [1359788400000, 123],
                      [1359788300000, 13.2],
                      [1359788410000, 23.1]
                    ],
      tags => {
          host        => "server1",
          data_center => "DC1"
      },
      ttl => 300
 };

 # Multiple data points, multi-dimension
 push @datapoints, {
      name => "impedance",
      type => "complex-number",
      datapoints => [
          [
              1359788400000,
              {
                  real      => 2.3,
                  imaginary => 3.4
              }
          ],
          [
              1359788300000,
              {
                  real      => 1.1,
                  imaginary => 5
              }
          ]
      ],
      tags => {
          host        => "server1",
          data_center => "DC1"
      }
 };

 my $result = $kdb->datapoints(\@datapoints);
 my $result = $kdb->datapoints(@datapoints);

Records metric data points.

See L<https://kairosdb.github.io/docs/build/html/restapi/AddDataPoints.html>

B<Parameters>

An array (or array ref) of datapoints, each is a hash reference.

There are three formats for datapoints as shown above, which can be mixed and matched.

This hash ref contains the following:

=over 4

=item name

Metric names must be unique. Multiple words in a metric name are typically
separated using an underscore ("_") to separate words such as archive_search.

This option is required.

=item timestamp

The timestamp is the date and time when the data was measured. It's a numeric
value that is the number of milliseconds since January 1st, 1970 UTC.

This option is required if a single I<value> option is provided.

=item value

The value is a number (i.e, 523 or 132.45).

You must provide either this option or I<datapoints>.

=item datapoints

An array reference of data points. Each data point consists of a I<timestamp> and I<value>.

You must provide either this option or a single I<value>.

=item tags

The tags field is a list of named properties. The tags are used when querying metrics to
narrow down the search.

For example, if multiple metrics are measured on server1, you could add the "host" => "server1"
tag to each of the metrics and queries could return all metrics for the "host" tagged with
the value of "server1".

At least one tag is required.

=item type

Type identifies custom data types. This field is only needed if the data value is something
other than a number. The type field is the name of the registered type for the custom data.

See L<Custom Types|https://kairosdb.github.io/docs/build/html/kairosdevelopment/CustomData.html> for information on custom types.

=item ttl

Sets the Cassandra ttl for the data points. In the example above the data points for metric
I<archive_file_tracked> will have the ttl set for 5 min.

Ommitting the ttl or setting it to 0 will use the default TTL value specified in settings.

=back

B<Returns>

Merely returns true (1) if successful.

B<Exceptions>

See also L<Net::KairosDB::REST::Error>

=over 4

=item 400 - Request Invalid

=item 500 - Internal Server Error

=back

=cut

sub delete_datapoints {

    my $self = shift;

    my $datapoints = [];
    if (scalar @_ == 1) {
        $datapoints = shift
    }
    elsif (scalar @_ > 1) {
        $datapoints = [ @_ ]
    }
    else {
        die 'No "datapoints" provided'
    }

    for my $args (@$datapoints) {
        die 'All datapoints must be a hashref'
            if ref $args ne 'HASH';

        # name
        die 'All datapoints must have a "name"'
            unless $args->{name};
        die 'The "name" option must be a string'
            if ref $args->{name};

        # type
        if ($args->{type}) {
            die 'The "type" option must be a string'
                if ref $args->{type};
        }

        # ttl
        if ($args->{ttl}) {
            die 'The "ttl" option must be a string'
                if ref $args->{ttl};
            die 'The "ttl" option must be in seconds'
                if $args->{ttl} =~ m/[^0-9]/;
        }

        # value
        if ($args->{value}) {
            die 'Each datapoint may have a "value" or a list of "datapoints", not both'
                if $args->{datapoints};
            my $ts = $args->{timestamp};
            $ts = _epoch_from_obj( $ts )
                if ref $ts;
            if ($ts) {
                die 'The "timestamp" option must be milliseconds since epoch'
                    if $ts =~ m/[^0-9]/;
            }
            else {
                $ts = _now_milliseconds()
            }
            $args->{timestamp} = $ts;
        }

        # OR datapoints
        if ($args->{datapoints}) {
            die 'The "timestamp" option is only valid with a single "value", not a list of "datapoints"'
                if $args->{timestamp};
            die 'The "datapoints" option must be an array ref'
                if $args->{datapoints} ne 'ARRAY';
            for my $dp (@{$args->{datapoints}}) {
                die 'All values in "datapoints" array must be array refs'
                    if $dp ne 'ARRAY';
                die 'Two values (timestamp, value) must be provided for each item in the "datapoints" array'
                    if scalar @$dp != 2;
                $dp->[0] = _epoch_from_obj( $dp->[0] )
                    if ref $dp->[0];
            }
        }

        # tags
        die 'The "tags" option is required'
            unless $args->{tags};
        die 'The "tags" option must be a hash ref'
            if $args->{tags} ne 'HASH';
        die 'The "tags" option must have at lease one value'
            if scalar keys %{$args->{tags}} > 0;

        for my $v ( values %{$args->{tags}} ) {
            die 'The "tags" values must all be strings'
                if ref $v;
        }

    } # for my $args (@$datapoints)

    return $self->post(
        $self->_mkuri('datapoints'),
        $datapoints
    )

} # sub delete_datapoints

=head2 query_metrics

 my $result = $kdb->query_metrics({
   "start_absolute" => 1357023600000,
   "end_relative" => {
       "value" => "5",
       "unit" => "days"
   },
   "time_zone" => "Asia/Kabul",
   "metrics" => [
       {
           "tags" => {
               "host" => ["foo", "foo2"],
               "customer" => ["bar"]
           },
           "name" => "abc.123",
           "limit" => 10000,
           "aggregators" => [
               {
                   "name" => "sum",
                   "sampling" => {
                       "value" => 10,
                       "unit" => "minutes"
                   }
               }
           ]
       },
       {
           "tags" => {
               "host" => ["foo", "foo2"],
               "customer" => ["bar"]
           },
           "name" => "xyz.123",
           "aggregators" => [
               {
                   "name" => "avg",
                   "sampling" => {
                       "value" => 10,
                       "unit" => "minutes"
                   }
               }
           ]
       }
   ]
 });

Returns a list of metric values based on a set of criteria. Also returns a set of all tag names and values that are found across the data points.

See L<http://kairosdb.github.io/docs/build/html/restapi/QueryMetrics.html>

B<Query Parameters>

A hash ref containing the following;

=over 4

=item start_absolute

The time in millseconds.

This option is required.

Note: You must specify either I<start_absolute> or I<start_relative> but not both. Similarly, you may specify either I<end_absolute> or I<end_relative> but not both. If either end time is not specified the current date and time is assumed.

=item start_relative

The relative start time is the current date and time minus the specified value and unit. Possible unit values are "milliseconds", "seconds", "minutes", "hours", "days", "weeks", "months", and "years". For example, if the start time is 5 minutes, the query will return all matching data points for the last 5 minutes.

=item end_absolute

The time in milliseconds. This must be later in time than the start time. If not specified, the end time is assumed to be the current date and time.

=item end_relative

The relative end time is the current date and time minus the specified value and unit. Possible unit values are "milliseconds", "seconds", "minutes", "hours", "days", "weeks", "months", and "years". For example, if the start time is 30 minutes and the end time is 10 minutes, the query returns matching data points that occurred between the last 30 minutes up to and including the last 10 minutes. If not specified, the end time is assumed to the current date and time.

=item time_zone

The time zone for the time range of the query. If not specified, UTC is used.

=item cache_time

The amount of time in seconds to re use the cache from a previous query. When a query is made Kairos looks for the cache file for the query. If a cache file is found and the timestamp of the cache file is within cache_time seconds from the current query, the cache is used.

Cache files are identified by hashing the metric name, the start and end time of the query and any tags specified. For example if you query a metric using relative start of 4 hours ago and then 30 min later you run the same query with a cache_time set to 2000 (just over 30 min) you will get the cached data back.

Sending a query with a cache_time set to 0 will always refresh the cache with new data from Cassandra.

Changing aggregators on a query does not effect the use of cache.

=item metrics

An array ref of hash refs as described as follows.

This option is required.

=over 4

=item name

The name of the metric(s) to return data points for.

This option is required.

=item aggregators

This is an ordered array ref of aggregators, each is a hash ref. They are processed in the order specified. The output of an aggregator is passed to the input of the next until all have been processed.

If no aggregator is specified, then all data points are returned.

Most aggregators support downsampling. Downsampling allows you to reduce the sampling rate of the data points and aggregate these values over a longer period of time. For example, you could average all daily values over the last week. Rather than getting 7 values you would get one value which is the average for the week. Sampling is specified with a "value" and a "unit".

Each hash ref contains the following:

=over 4

=item value

An integer value.

=item unit

The time range. Possible unit values are "milliseconds", "seconds", "minutes", "hours", "days", "weeks", "months", and "years".

=item align_sampling

An optional property. Setting this to true will cause the aggregation range to be aligned based on the sampling size. For example if your sample size is either milliseconds, seconds, minutes or hours then the start of the range will always be at the top of the hour. The effect of setting this to true is that your data will take the same shape when graphed as you refresh the data. This is false by default. Note that align_sampling and align_start_time are mutually exclusive. If more than one are set, unexpected results will occur.

=item align_start_time

An optional property. When set to true the time for the aggregated data point for each range will fall on the start of the range instead of being the value for the first data point within that range. This is false by default. Note that align_sampling, align_start_time, and align_end_time are mutually exclusive. If more than one are set, unexpected results will occur.

=item align_end_time

An optional property. Setting this to true will cause the aggregation range to be aligned based on the sampling size. For example if your sample size is either milliseconds, seconds, minutes or hours then the start of the range will always be at the top of the hour. The difference between align_start_time and align_end_time is that align_end_time sets the timestamp for the datapoint to the beginning of the following period versus the beginning of the current period. As with align_start_time, setting this to true will cause your data to take the same shape when graphed as you refresh the data. Note that align_start_time and align_end_time are mutually exclusive. If more than one are set, unexpected results will occur.

=item start_time

An optional property. Used along with align_start_time. This is the alignment start time. This defaults to 0

=back

=item tags

Tags narrow down the search. Only metrics that include the tag and matches one of the values are returned. Tags is optional.

=item group_by

The resulting data points can be grouped by one or more tags, a time range, or by value, or by a combination of the three.

The "group_by" property in the query is an array ref of one or more groupers which are hash refs. Each grouper has a name and then additional properties specific to that grouper.

See Grouping by Tags for information on grouping by tags.

See Grouping by Time for information on how to group by a time range.

See Grouping by Value for information on how to group by data point values.

See Grouping by Bins for information on how to group by bins.

Note: grouping by a time range, by value, or by bins can slow down the query.

=item exclude_tags

By default, the result of the query includes tags and tag values associated with the data points. If exclude_tags is set to true, the tags will be excluded from the response.

=item limit

Limits the number of data points returned from the data store. The limit is applied before any aggregator is executed.

=item order

Orders the returned data points. Values for order are "asc" for ascending or "desc" for descending. Defaults to ascending.

This sorting is done before any aggregators are executed.

=back

=back

B<Returns>

Returns the metrics as a heshref.

Something like this:

 {
   'queries' => [
     {
       'results' => [
         {
           'group_by' => [
             {
               'name' => 'type',
               'type' => 'number'
             }
           ],
           'name' => 'collectd.cpu',
           'tags' => {
             'host' => [
               'my.server.local'
             ],
             'plugin_instance' => [
               '0','1','2','3'
             ],
             'type' => [
               'count',
               'percent'
             ],
             'type_instance' => [
               'idle',
               'interrupt',
               'nice',
               'softirq',
               'steal',
               'system',
               'user',
               'wait'
             ]
           },
           'values' => [
             [
               '1593325746247',
               '99.7997997997998'
             ],
             [
               '1593325746247',
               '99.7993981945838'
             ],
             [
               '1593325746247',
               '99.6996996996997'
             ]
           ]
         }
       ],
       'sample_size' => 537
     }
   ]
 }

Version 0.9.4 of KairosDB includes a group_by named "type". The type is the custom data type. If the data returned is not a custom type then "number" is returned. See L<Custom Types|https://kairosdb.github.io/docs/build/html/kairosdevelopment/CustomData.html> for information on custom types.

B<Exceptions>

See also L<Net::KairosDB::REST::Error>

=over 4

=item 400 - Request Invalid

=item 500 - Internal Server Error

=back

=cut

sub query_metrics {

    my $self = shift;

    my $query;
    if (scalar @_ == 1) {
        $query = shift;
        die 'Query must be a hashref'
            unless ref $query eq 'HASH';
    }
    elsif (scalar @_ > 1) {
        $query = { @_ }
    }
    else {
        die 'No Query provided'
    }

    ## Query Properties

    # start_absolute start_relative end_absolute end_relative
    die 'All Query must have a "start_absolute" or "start_relative"'
        unless $query->{start_absolute}
            or $query->{start_relative};

    for my $ii (qw/ start_absolute end_absolute /) {
        next unless $query->{$ii};
        die qq|The "$ii" must be a number|
            if ref $query->{$ii}
               or $query->{$ii} =~ m/[^0-9]/
    }

    for my $ii (qw/ start_relative end_relative /) {
        next unless $query->{$ii};
        die qq|The "$ii" must be a hashref|
            unless ref $query->{$ii} eq 'HASH';
        if (defined $query->{$ii}->{value}) {
            die qq|The "$ii" must have a "value" which is a number|
                if ref $query->{$ii}->{value}
                    or $query->{$ii}->{value} =~ m/[^0-9]/;
        }
        else {
            die qq|The "$ii" must have a "value" which is a number|
        }
        if (defined $query->{$ii}->{unit}) {
            die qq|The "$ii" must have a "unit" which is a string|
                if ref $query->{$ii}->{unit}
        }
        else {
            die qq|The "$ii" must have a "unit" which is a string|
        }
    } # for my $ii (qw/ start_relative end_relative /)

    # time_zone cache_time
    for my $ii (qw/ time_zone cache_time /) {
        die qq|The "$ii" must be a string, if provided|
            if $query->{$ii}
            and ref $query->{$ii};
    }

    ## Metric Properties
    die 'All Query must have "metrics"'
        unless $query->{metrics};
    die 'The "metrics" parameter must be an arrayref'
        unless ref $query->{metrics} eq 'ARRAY';

    for my $args (@{$query->{metrics}}) {
        die 'All "metrics" values must be a hashref'
            if ref $args ne 'HASH';

        # name
        die 'All metrics must have a "name"'
            unless $args->{name};
        die 'The metrics "name" option must be a string'
            if ref $args->{name};

        # aggregators
        if (defined $args->{aggregators}) {
            die 'The metrics "aggregators" option must be an array ref'
                unless ref $args->{aggregators} eq 'ARRAY';
            die 'The metrics "aggregators" option must have at lease one value'
                unless scalar @{$args->{aggregators}} > 0;
            for my $v (@{$args->{aggregators}}) {
                die 'The matrics "aggregators" values must all be HASH refs'
                    unless ref $v eq 'HASH';

                # values
                die q|The metrics "aggregators" must have a "values" option|
                    unless defined $v->{values};
                die q|The metrics "aggregators" "values" option must be a number|
                    if ref $v or $v->{values} =~ m/[^0-9]/;

                # unit
                die q|The metrics "aggregators" must have a "unit" option|
                    unless defined $v->{unit};
                die q|The metrics "aggregators" "values" option must be a number|
                    if ref $v or $v->{unit} =~ m/^(?:milliseconds|seconds|minutes|hours|days|weeks|months|years)$/;

                # align_sampling align_start_time align_end_time
                if ( defined $v->{align_sampling} ) {
                    die q|The metrics "aggregators" values must NOT have both "align_sampling" and "align_start_time"|
                        if ($v->{align_sampling} and $v->{align_start_time});
                    $args->{align_sampling} = $args->{align_sampling} ? JSON->true : JSON->false
                }
                if ( defined $v->{align_start_time} ) {
                    die q|The metrics "aggregators" values must NOT have both "align_start_time" and "align_end_time"|
                        if ($v->{align_start_time} and $v->{align_end_time});
                    $args->{align_start_time} = $args->{align_start_time} ? JSON->true : JSON->false

                }
                if ( defined $v->{align_end_time} ) {
                    $args->{align_end_time} = $args->{align_end_time} ? JSON->true : JSON->false
                }
                if ( defined $v->{start_time} ) {
                    die q|The metrics "aggregators" "start_time" option is not useful without "align_start_time"|
                        unless $v->{align_start_time};
                    die q|The metrics "aggregators" "start_time" option must be a number|
                        if ref $v or $v->{start_time} =~ m/[^0-9]/;
                }

            } # for my $v (@{$args->{aggregators}})
        } # if (defined $args->{aggregators})

        # tags
        if ($args->{tags}) {
            die 'The metrics "tags" option must be a hash ref'
                unless ref $args->{tags} eq 'HASH';
            die 'The metrics "tags" option must have at lease one value'
                unless scalar keys %{$args->{tags}} > 0;
            for my $v ( map { ref $_ eq 'ARRAY' ? @$_ : $_ } values %{$args->{tags}} ) {
                die 'The metrics "tags" values must all be strings'
                    if ref $v;
            }
        }

        # group_by
        if (defined $args->{group_by}) {
            die 'The metrics "group_by" option must be an array ref'
                unless ref $args->{group_by} eq 'ARRAY';
            die 'The metrics "group_by" option must have at lease one value'
                unless scalar @{$args->{group_by}} > 0;
            for my $v (@{$args->{group_by}}) {
                die 'The matrics "group_by" values must all be HASH refs'
                    unless ref $v eq 'HASH';

                # name
                die 'All metrics "group_by" must have a "name"'
                    unless $args->{name};
                die 'The metrics "group_by" "name" option must be a string'
                    if ref $args->{name};

                # TODO more group_by options http://kairosdb.github.io/docs/build/html/restapi/QueryMetrics.html#request-methods

            }
        }

        # exclude_tags
        if (defined $args->{exclude_tags}) {
            die 'The metrics "exclude_tags" option must be a scalar'
                if ref $args->{exclude_tags};
            $args->{exclude_tags} = $args->{exclude_tags} ? JSON->true : JSON->false
        }

        # limit
        die qq|The metrics "limit" option must be a number|
            if $args->{limit}
                and $args->{limit} =~ m/[^0-9]/;

        # order
        die qq|The metrics "order" option if provided must be "asc" or "desc" only|
            if $args->{order}
                and $args->{order} !~ m/^(asc|desc)$/;


    } # for my $args (@{$query->{metrics}})


    return $self->post(
        $self->_mkuri(qw/ datapoints query /),
        $query
    )

} # sub query_metrics


# TODO query metric tags

=head2 features

 my @features = $kdb->feature();
 my $features = $kdb->feature();
 my $feature  = $kdb->feature('featurename');

The Features API returns metadata about various components of KairosDB. For example, this API will return metadata about aggregators and GroupBys.

See L<https://kairosdb.github.io/docs/build/html/restapi/Features.html>

B<Parameters>

A string 'featurename' is optional, requesting details on just one feature.

Generally the two high level features are 'aggregators' and 'sampling'.

B<Returns>

A list of L<Net::KairosDB::REST::Feature> Objects.

Or if a I<featurename> is provided, an object or nothing.

=cut

sub features {
    my $self = shift;
    my $feature = shift;
    my $data = $self->get( $self->_mkuri('features',($feature ? $feature : ())));
    return if ref $data ne 'ARRAY';
    my @foo = map { Net::KairosDB::REST::Feature->new($_) } @$data;
    if ($feature) {
        return $foo[0] if @foo;
        return
    }
    return wantarray ? @foo : \@foo;
}

=head2 health

 my @healths = $kdb->health();
 my $healths = $kdb->health();

KairosDB provides REST APIs that show the health of the system.

There are currently two health checks executed for each API.

- The JVM thread deadlock check verifies that no deadlocks exist in the KairosDB JVM.
- The Datastore query check performs a query on the data store to ensure that the data store is responding

See L<https://kairosdb.github.io/docs/build/html/restapi/Health.html>

B<Parameters>

None.

B<Returns>

List of health status items

=cut

sub health {
    my $self = shift;
    my $data = $self->get( $self->_mkuri('health','status') );
    return unless $data;
    return wantarray ? @$data : $data
}

=head2 get_metadata

 my @servicekeys = $kdb->metadata( service => 'service.name' );

 my @keys        = $kdb->metadata( service    => 'service.name',
                                   servicekey => 'service.key' );

 my $value       = $kdb->metadata( service    => 'service.name',
                                   servicekey => 'service.key',
                                   key        => 'key' );

This function returns metadata information.

The Metadata Rest API is a way to write data to the datastore in name/value pairs. Data is written separate from the time series data. Metadata is partitioned by a service name. A service partition can have multiple service keys. Each service key holds name/value pairs. A value is a string.

B<Example Scenario>

Assume you have a service that maintains metadata about each metric. Let’s call it the I<Metric Service>. Your service associates each metric with a description, owner, and the unit type. The B<service name> is I<Metric Service>, the I<Metric> is the B<service key> and the B<name/value> pairs are the I<owner>, I<unit>, and I<description> and their I<values>.

 Metric Service

 Metric          Owner      Unit   Description
 ------------------------------------------------------
 disk.available  OP's team  MB     Available disk space
 foo.throughput  Foo team   Bytes  Number of bytes

Translates to

 Service: Metric Service
 Service Key: disk.available
 name: Owner
 value: Op's team
 name: Unit
 value: MB
 name: Description
 value: Available disk space

B<Parameters>

=over 4

=item service

The name of the service.

This option is required.

=item servicekey

The name of the service key.

This is an optional argument.

=item key

The name of the key.

This is an optional argument and is ignored without a I<servicekey>.

=back

B<Returns>

A list or arrayref of service or service key or key names.

If I<service>, I<servicekey>, and I<key> are provided, the return value will be a string.

=cut

sub get_metadata {
    my ($self, %args) = @_;
    die 'No "service" provided'
        unless $args{service};
    my @path = ('metadata', $args{service});
    if ($args{servicekey}) {
        push @path, $args{servicekey};
        push @path, $args{key}
            if $args{key}
    }
    # if all three arguments, need to do special handling
    # ... see https://github.com/kairosdb/kairosdb/issues/624
    if (4 == scalar @path) {
        my $path = $self->_mkuri(@path);
        my $text;
        eval {
        $text = $self->get( $path, undef, deserializer => undef );
        };
        # Catch error 500 which is a bug
        # ... see https://github.com/kairosdb/kairosdb/issues/625
        if ($@) {
            return if $@->code == 500;
            die $@
        }
        return $text
    }
    my $data = $self->get( $self->_mkuri(@path) );
    return unless $data->{results};
    # strangely, keys are returned many times. hide that.
    return wantarray ? uniq sort @{$data->{results}}
                     : [ uniq sort @{$data->{results}} ]
}

=head2 add_metadata

 my $result = $kdb->add_metadata(
                   service    => 'service.name',
                   servicekey => 'service.key',
                   key        => 'key.name',
                   value      => 'My text data'
               );

Add a value to service metadata.

B<Parameters>

=over 4

=item service

The name of the service.

This option is required.

=item servicekey

The name of the service key.

This option is required.

=item key

The name of the key.

This option is required.

=item value

The value of the key.

This option is required.

=back

B<Returns>

Merely returns true (1) if successful.

B<Exceptions>

See also L<Net::KairosDB::REST::Error>

=over 4

=item 500 - Internal server error

=back

=cut

sub add_metadata {
    my ($self, %args) = @_;
    for my $foo (qw/ service servicekey key value/) {
        die sprintf('No "%s" provided', $foo)
            unless $args{$foo};
    }
    my @path = (
        'metadata',
        $args{service},
        $args{servicekey},
        $args{key});

    return $self->post(
        $self->_mkuri(@path),
        $args{value},
        serializer => undef, # send as is
    )

}

=head2 delete_metadata

 my $result = $kdb->delete_metadata(
                   service    => 'service.name',
                   servicekey => 'service.key',
                   key        => 'key.name',
               )

Delete a value from service metadata.

B<Parameters>

=over 4

=item service

The name of the service.

This option is required.

=item servicekey

The name of the service key.

This option is required.

=item key

The name of the key.

This option is required.

=back

B<Returns>

Returns all keys for the given service key or an empty list if no keys exist.

B<Exceptions>

See also L<Net::KairosDB::REST::Error>

=over 4

=item 500 - Internal server error

=back

=cut

sub delete_metadata {
    my ($self, %args) = @_;
    for my $foo (qw/ service servicekey key /) {
        die sprintf('No "%s" provided', $foo)
            unless $args{$foo};
    }
    my @path = (
        'metadata',
        $args{service},
        $args{servicekey},
        $args{key});

    return $self->delete( $self->_mkuri(@path) );
}

=head2 metricnames

 my @names = $kdb->metricnames();
 my $names = $kdb->metricnames();

 my @names = $kdb->metricnames( prefix => 'foo' );

Returns a list of all metric names.

(Metric isn't substantial enough in the REST API to warrant an object)

See L<https://kairosdb.github.io/docs/build/html/restapi/ListMetricNames.html>

B<Parameters>

=over 4

=item prefix

If you specify the I<prefix> parameter, only names that start with prefix are returned.

This is an optional argument.

=back

B<Returns>

A list or arrayref of metric names.

=cut

sub metricnames {
    my ($self, %args) = @_;
    my $query = ''; # So no warning when concat'd
    if ($args{prefix}) {
        $query = '?prefix=' . $args{prefix}
    }
    my $data = $self->get( $self->_mkuri('metricnames') . $query );
    return unless $data->{results};
    return wantarray ? sort @{$data->{results}} : [ sort @{$data->{results}} ]
}

=head2 delete_metric

  my $result = $kdb->delete_metric('some.metric.name');

Deletes a metric and B<all data points> associated with the metric.

Note: Delete works for the Cassandra and H2 data stores only.

(Metric isn't substantial enough in the REST API to warrant an object)

See L<https://kairosdb.github.io/docs/build/html/restapi/DeleteMetric.html>

B<Parameters>

=over 4

=item metric name

The first argument is the name of the metric to be deleted, as a scalar.

=back

B<Returns>

Merely returns true (1) if successful.

B<Exceptions>

See also L<Net::KairosDB::REST::Error>

=over 4

=item 400 - Request invalid

=item 500 - Internal server error

=back

=cut

sub delete_metric {
    my ($self, $name) = @_;
    return unless $name;
    return $self->delete( $self->_mkuri('metric',$name) );
    # return $self->get( $self->_mkuri('metric',$name) );
}

# FIXME pod
# See https://kairosdb.github.io/docs/build/html/restapi/Roll-ups.html#list-roll-up-tasks
sub rollups {
    my $self = shift;
    my $data = $self->get( $self->_mkuri('rollups') );
    # TODO make sure this works
    return $data
}

# TODO create roll-up tasks
# TODO get roll-up task
# TODO delete roll-up task
# TODO update roll-up task

=head2 tagnames

FIXME

See L<https://kairosdb.github.io/docs/build/html/restapi/ListTagNames.html>

=cut

sub tagnames {
    my $self = shift;
    my $data = $self->get( $self->_mkuri('tagnames') );
    return $data->{results} if $data->{results};
    return
}

=head2 tagvalues

FIXME

See L<https://kairosdb.github.io/docs/build/html/restapi/ListTagValues.html>

=cut

sub tagvalues {
    my $self = shift;
    my $data = $self->get( $self->_mkuri('tagvalues') );
    return $data->{results} if $data->{results};
    return
}

=head2 server_version

Returns the version of KairosDB server

See L<https://kairosdb.github.io/docs/build/html/restapi/Version.html>

B<Parameters>

None.

B<Returns>

KairosDB server version as a scalar (string)

=cut

sub server_version {
    my $self = shift;
    my $data = $self->get( $self->_mkuri('version') );
    return $data->{version} if $data->{version};
    return
}


1;
