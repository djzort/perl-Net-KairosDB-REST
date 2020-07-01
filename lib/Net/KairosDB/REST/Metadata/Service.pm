#!perl
# vim: softtabstop=4 tabstop=4 shiftwidth=4 ft=perl expandtab smarttab
# ABSTRACT: Perl API for KairosDB - Metadata/Service object

package Net::KairosDB::REST::Metadata::Service;

use v5.10;
use Moo;
use Net::KairosDB::REST::Feature::Metadata::Service;
# VERSION
use namespace::clean;

has 'service' => ( is => 'ro' );
has 'kdb'     => ( is => 'rwp', weak_ref => 1 );

sub _looks_ok {
    my ($args,$keys) = @_;
    for my $k (@$keys) {
        return unless defined $args->{$k};
        return unless length $args->{$k};
    }
    return 1
}

# See https://kairosdb.github.io/docs/build/html/restapi/Metadata.html#list-keys
sub service_keys {
    my $self = shift;
    my $kdb = $self->kdb;
    my $data = $kdb->get(
        $kdb->_mkuri(
            'metadata', $self->service ) );
    return if ref $data ne 'ARRAY';
    return wantarray ? @$data : $data
}

# https://kairosdb.github.io/docs/build/html/restapi/Metadata.html#list-keys
sub keys {
    my ($self,%args) = @_;
    return unless _looks_ok(\%args,[qw/ service_key /];
    my $kdb = $self->kdb;
    my $data = $kdb->get(
        $kdb->_mkuri(
            'metadata', $self->service, $args{service_key} ) );
    return if ref $data ne 'ARRAY';
    return wantarray ? @$data : $data
}

# https://kairosdb.github.io/docs/build/html/restapi/Metadata.html#add-the-value
sub add {
    my ($self,%args) = @_;
    return unless _looks_ok(\%args,[qw/ service_key key value /];
    my $kdb = $self->kdb;
    my $data = $kdb->post(
        $kdb->_mkuri(
            'metadata', $self->service,
            $args{service_key}, $args{key} ),
        $args{value});
    return $data
}

# https://kairosdb.github.io/docs/build/html/restapi/Metadata.html#get-the-value
sub get {
    my ($self,%args) = @_;
    return unless _looks_ok(\%args,[qw/ service_key key /];
    my $kdb = $self->kdb;
    my $data = $kdb->get(
        $kdb->_mkuri(
            'metadata', $self->service,
            $args{service_key}, $args{key} ) );
    return $data
}

# https://kairosdb.github.io/docs/build/html/restapi/Metadata.html#delete-key
sub delete {
    my ($self,%args) = @_;
    return unless _looks_ok(\%args,[qw/ service_key key /];
    my $kdb = $self->kdb;
    my $data = $kdb->delete(
        $kdb->_mkuri(
            'metadata', $self->service,
            $args{service_key}, $args{key} ) );
    return $data
}


1;

