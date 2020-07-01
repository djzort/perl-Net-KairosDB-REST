#!perl
# vim: softtabstop=4 tabstop=4 shiftwidth=4 ft=perl expandtab smarttab
# ABSTRACT: Perl API for KairosDB - Feature/Property/Property Object

package Net::KairosDB::REST::Feature::Property::Validation;

use v5.10;
use Moo;
# VERSION
use namespace::clean;

has 'expressions' => ( is => 'ro' );
has 'type'        => ( is => 'ro' );
has 'message'     => ( is => 'ro' );

1;

