#!perl
# vim: softtabstop=4 tabstop=4 shiftwidth=4 ft=perl expandtab smarttab
# ABSTRACT: Perl API for KairosDB - Feature Object

package Net::KairosDB::REST::Feature;

use Moo;
# VERSION
use Net::KairosDB::REST::Feature::Property;
use namespace::clean;

has 'name'       => ( is => 'ro' );
has 'label'      => ( is => 'ro' );
has 'properties' => (
    is     => 'ro',
    coerce => sub {
        return [
            map { Net::KairosDB::REST::Feature::Property->new($_) } @{$_[0]}
        ]
    }
);

1;

