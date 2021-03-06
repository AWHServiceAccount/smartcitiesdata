defmodule DiscoveryApiWeb.Plugs.VerifyHeaderAuth0Test do
  use ExUnit.Case
  use Placebo

  use DiscoveryApiWeb.ConnCase
  alias DiscoveryApiWeb.Plugs.VerifyHeaderAuth0
  alias DiscoveryApi.Auth.Auth0.CachedJWKS

  @verified_conn Guardian.Plug.put_current_token(%Plug.Conn{}, "fake_token")
  @unverified_conn %Plug.Conn{}
  @opts [{:fake, "options"}]

  describe "init/1" do
    test "delegates to Guardian.Plug.VerifyHeader" do
      allow(Guardian.Plug.VerifyHeader.init(any()), return: :init_result)

      result = VerifyHeaderAuth0.init(:init_arg)

      assert result == :init_result
      assert_called(Guardian.Plug.VerifyHeader.init(:init_arg), times(1))
    end
  end

  describe "call/2" do
    test "delegates to Guardian.Plug.VerifyHeader" do
      allow(Guardian.Plug.VerifyHeader.call(any(), any()), return: @verified_conn)

      result = VerifyHeaderAuth0.call(@unverified_conn, @opts)

      assert result == @verified_conn
      assert_called(Guardian.Plug.VerifyHeader.call(@unverified_conn, @opts), times(1))
    end

    test "retries call to Guardian.Plug.VerifyHeader when verification fails the first time for a cached jwks" do
      CachedJWKS.set(%{"keys" => []})
      allow(Guardian.Plug.VerifyHeader.call(any(), any()), return: @unverified_conn)

      result = VerifyHeaderAuth0.call(@unverified_conn, @opts)

      assert result == @unverified_conn
      assert_called(Guardian.Plug.VerifyHeader.call(@unverified_conn, @opts), times(2))
    end

    test "invalidates cached jwks when verification fails" do
      CachedJWKS.set(%{"keys" => []})
      allow(Guardian.Plug.VerifyHeader.call(any(), any()), return: @unverified_conn)

      VerifyHeaderAuth0.call(@unverified_conn, @opts)

      assert CachedJWKS.get() == nil
    end

    test "does not retry call to Guardian.Plug.VerifyHeader when jwks was not cached" do
      CachedJWKS.delete()
      allow(Guardian.Plug.VerifyHeader.call(any(), any()), return: @unverified_conn)

      result = VerifyHeaderAuth0.call(@unverified_conn, @opts)

      assert result == @unverified_conn
      assert_called(Guardian.Plug.VerifyHeader.call(@unverified_conn, @opts), times(1))
    end
  end
end
