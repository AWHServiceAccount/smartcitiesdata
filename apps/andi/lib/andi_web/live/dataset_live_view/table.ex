defmodule AndiWeb.DatasetLiveView.Table do
  @moduledoc """
    LiveComponent for dataset table
  """
  use Phoenix.LiveComponent

  def render(assigns) do
    down_arrow = '&#9660;'
    up_arrow = '&#9650;'

    ~L"""
    <div id="<%= @id %>" class="datasets-index__table">
      <table class="datasets-table">
        <thead>
        <th class="datasets-table__th datasets-table__cell datasets-table__th--<%= Map.get(@order, "data-title", "unsorted") %>" phx-click="order-by" phx-value-field="data-title">Dataset Name </th>
        <th class="datasets-table__th datasets-table__cell datasets-table__th--<%= Map.get(@order, "org-title", "unsorted") %>" phx-click="order-by" phx-value-field="org-title">Organization </th>
        </thead>
        <%= if @datasets == [] do %>
          <tr><td class="datasets-table__cell" colspan="100%">No Datasets Found</td></tr>
        <% else %>
          <%= for dataset <- @datasets do %>
          <tr class="datasets-table__tr">
          <td class="datasets-table__cell datasets-table__cell--break"><%= dataset["data-title"] %></td>
            <td class="datasets-table__cell datasets-table__cell--break"><%= dataset["org-title"] %></td>
          </tr>
          <% end %>
        <% end %>
      </table>
    </div>
    """
  end

  def handle_event("order-by", %{"field" => field}, socket) do
    send(self(), {:order, field})
    {:noreply, socket}
  end
end
