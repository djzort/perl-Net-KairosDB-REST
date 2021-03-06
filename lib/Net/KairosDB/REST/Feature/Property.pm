#!perl
# vim: softtabstop=4 tabstop=4 shiftwidth=4 ft=perl expandtab smarttab
# ABSTRACT: Perl API for KairosDB - Feature/Property Object

package Net::KairosDB::REST::Feature::Property;

use v5.10;
use Moo;
use Net::KairosDB::REST::Feature::Property::Property;
# VERSION
use namespace::clean;

has 'name'        => ( is => 'ro' );
has 'label'       => ( is => 'ro' );
has 'description' => ( is => 'ro' );
has 'properties' => (
    is     => 'ro',
    coerce => sub {
        return [
            map { Net::KairosDB::REST::Feature::Property::Property->new($_) } @{$_[0]}
        ]
    }
);


1;

