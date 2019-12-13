#!perl
# vim: softtabstop=4 tabstop=4 shiftwidth=4 ft=perl expandtab smarttab
# ABSTRACT: Perl API for KairosDB - Error Object

package Net::KairosDB::REST::Error;

use Moo;
with 'Throwable';
# VERSION
use namespace::clean;

has code => (is => 'ro');
has message => (is => 'ro');
has content => (is => 'ro');

1;

=pod

=encoding utf-8

=cut

=head1 SYNOPSIS

 my $obj = Net::KairosDB::REST->new( %args );
 eval {
     $obj->something('arg');
 };

 if ($@) {
    # $@ is a Net::KairosDB::REST::Error object
    warn $@->code, "\n";
    warn $@->message, "\n";
    exit
 }

=head1 DESCRIPTION

A simple object that is thrown when things go wrong.

Inherits from L<Throwable>.

=head1 FUNCTIONS

=head2 throw

This is inherited from L<Throwable>, and it's utility is only internal to
the I<Net::KairosDB::REST> functions. Users typically won't call it.

A I<code> and I<message> argument are required.

=head1 OBJECT METHODS

This is what users of I<Net::KairosDB::REST> should be concerned with.

=head2 code

The error code number.

=head2 message

The error message.

=head2 previous_exception

Inherited from L<Throwable>. Don't rely upon this, just use for debugging.

=cut
