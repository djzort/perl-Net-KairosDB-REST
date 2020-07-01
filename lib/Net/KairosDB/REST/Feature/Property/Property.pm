#!perl
# vim: softtabstop=4 tabstop=4 shiftwidth=4 ft=perl expandtab smarttab
# ABSTRACT: Perl API for KairosDB - Feature/Property/Property Object

package Net::KairosDB::REST::Feature::Property::Property;

use v5.10;
use Moo;
# VERSION
use Net::KairosDB::REST::Feature::Property::Validation;
use namespace::clean;

has 'name'         => ( is => 'ro' );
has 'label'        => ( is => 'ro' );
has 'description'  => ( is => 'ro' );
has 'optional'     => ( is => 'ro' );
has 'type'         => ( is => 'ro' );
has 'options'      => ( is => 'ro' );
has 'defaultValue' => ( is => 'ro' );
has 'autocomplete' => ( is => 'ro' );
has 'multiline'    => ( is => 'ro' );
has 'validations'  => ( is => 'ro',
    coerce => sub {
        return [ map {
                Net::KairosDB::REST::Feature::Property::Validation->new($_)
            } @{$_[0]} ]
    }
);

has 'properties' => (
    is     => 'ro',
    coerce => sub {
        return [ map { __PACKAGE__->new($_) } @{$_[0]} ]
    }
);


1;

