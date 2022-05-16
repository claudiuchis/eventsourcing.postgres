using System;

namespace Eventuous.Postgres.Store;

public class WrongExpectedVersionException : Exception {
    public WrongExpectedVersionException(string stream): base($"Wrong expected version exception for stream {stream}") {}
}