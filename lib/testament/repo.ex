defmodule Testament.Repo do

    use Ecto.Repo,
        otp_app: :testament,
        adapter: Ecto.Adapters.MyXQL

end
